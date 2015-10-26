package algorithms.adaptiverepartitioning;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.util.Random;

/**
 * Implementation of the Adaptive Repartitioning (ARP) algorithm as described
 * in:
 * <p/>
 * "Adaptive Partitioning of Large-Scale Dynamic Graphs" (ICDCS14)
 * <p/>
 * To share global knowledge about the partition load and demand, the
 * algorithm exploits aggregators. For each of the k partitions, we use two
 * aggregators:
 * The first aggregator stores the capacity (CA_i) of partition i, the second
 * stores the demand (DA_i) for that partition (= number of vertices that want
 * to migrate in the next superstep).
 * <p/>
 * Superstep 1 Initialization Phase:
 * <p/>
 * If the input graph is an unpartitioned graph, the algorithm will at first
 * initialize each vertex with a partition-id i (hash based). This is skipped
 * if the graph is already partitioned.
 * After that, each vertex will notify CA_i that it is currently a member of the
 * partition and propagates the partition-id to its neighbours.
 * <p/>
 * The main computation is divided in two phases:
 * <p/>
 * Phase 2 Demand Phase (even numbered superstep):
 * <p/>
 * Based on the information about their neighbours, each vertex calculates its
 * desired partition, which is the most frequent one among the neighbours
 * (label propagation). If the desired partition and the actual partition are
 * not equal the vertex will notify the DA of the desired partition that the
 * vertex want to migrate in.
 * <p/>
 * Phase 3 Migration Phase (odd numbered superstep):
 * <p/>
 * Based on the information of the first phase, the algorithm will calculate
 * which vertices are allowed to migrate in their desired partition. If a vertex
 * migrates to another partition it notifies the new and old CA.
 * <p/>
 * The computation will terminate if no vertex wants to migrate, the maximum
 * number of iterations (configurable) is reached or each vertex reaches the
 * maximum number of partition switches (configurable).
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 * @see <a href="http://www.few.vu.nl/~cma330/papers/ICDCS14.pdf">Adaptive
 * Partitioning of Large-Scale Dynamic Graphs</a>
 */
public class ARPComputation implements
  GraphAlgorithm<Long, ARPVertexValue, NullValue, Graph<Long, ARPVertexValue,
    NullValue>> {
  private int maxIterations;
  private static int k;
  private static double capacityThreshold;
  public static final String CAPACITY_AGGREGATOR_PREFIX = "load.";
  public static final String DEMAND_AGGREGATOR_PREFIX = "demand.";
  private final VertexCentricConfiguration params;
  private static Random random;

  public ARPComputation(int maxIterations, int k, double threshold,
    VertexCentricConfiguration params) {
    this.maxIterations = maxIterations;
    ARPComputation.k = k;
    capacityThreshold = threshold;
    random = new Random();
    this.params = params;
  }

  /**
   * Graph run method to start the VertexCentricIteration
   *
   * @param graph graph that should be used for EPGMLabelPropagation
   * @return gelly Graph with partitioned vertices
   * @throws Exception
   */
  public Graph<Long, ARPVertexValue, NullValue> run(
    Graph<Long, ARPVertexValue, NullValue> graph) throws Exception {
    Graph<Long, ARPVertexValue, NullValue> undirectedGraph =
      graph.getUndirected();
    // initialize vertex values and run the Vertex Centric Iteration
    return undirectedGraph
      .runVertexCentricIteration(new ARPUpdater(), new ARPMessage(),
        maxIterations, params);
  }

  /**
   * Updates every vertex with its new partition and notifies all aggregators
   */
  public static final class ARPUpdater extends
    VertexUpdateFunction<Long, ARPVertexValue, Long> {
    /**
     * final long value +1
     */
    final long POSITIVE_ONE = 1;
    /**
     * final long value -1
     */
    final long NEGATIVE_ONE = -1;

    @Override
    public void updateVertex(Vertex<Long, ARPVertexValue> vertex,
      MessageIterator<Long> msg) throws Exception {
      if (getSuperstepNumber() == 1) {
        long newValue = vertex.getId() % k;
        String aggregator = CAPACITY_AGGREGATOR_PREFIX + newValue;
        notifyAggregator(aggregator, POSITIVE_ONE);
        setNewVertexValue(new ARPVertexValue(newValue, 0));
      } else {
        //odd numbered superstep 3 (migrate)
        if ((getSuperstepNumber() % 2) == 1) {
          long desiredPartition = vertex.getValue().getDesiredPartition();
          long currentPartition = vertex.getValue().getCurrentPartition();
          if (desiredPartition != currentPartition) {
            boolean migrate = doMigrate(desiredPartition);
            if (migrate) {
              migrateVertex(vertex, desiredPartition);
            }
          }
          String aggregator = CAPACITY_AGGREGATOR_PREFIX + currentPartition;
          notifyAggregator(aggregator, POSITIVE_ONE);
          //even numbered superstep 2 (demand)
        } else if ((getSuperstepNumber() % 2) == 0) {
          long desiredPartition = getDesiredPartition(vertex, msg);
          long currentPartition = vertex.getValue().getCurrentPartition();
          boolean changed = currentPartition != desiredPartition;
          if (changed) {
            String aggregator = DEMAND_AGGREGATOR_PREFIX + desiredPartition;
            notifyAggregator(aggregator, POSITIVE_ONE);
            setNewVertexValue(
              new ARPVertexValue(currentPartition, desiredPartition));
          }
          String aggregator = CAPACITY_AGGREGATOR_PREFIX + currentPartition;
          notifyAggregator(aggregator, POSITIVE_ONE);
        }
      }
    }

    /**
     * Notify Aggregator
     *
     * @param aggregatorString Aggregator name
     * @param v                value
     */
    private void notifyAggregator(final String aggregatorString, final long v) {
      LongSumAggregator aggregator = getIterationAggregator(aggregatorString);
      aggregator.aggregate(v);
    }

    /**
     * Calculates the partition the vertex wants to migrate to
     *
     * @param vertex   actual vertex
     * @param messages all messages
     * @return desired partition id the vertex wants to migrate to
     */
    private long getDesiredPartition(final Vertex<Long, ARPVertexValue> vertex,
      final Iterable<Long> messages) {
      long currentPartition = vertex.getValue().getCurrentPartition();
      long desiredPartition = vertex.getValue().getDesiredPartition();
      // got messages?
      if (messages.iterator().hasNext()) {
        // partition -> neighbours in partitioni
        long[] countNeighbours = getPartitionFrequencies(messages);
        // partition -> desire to migrate
        double[] partitionWeights =
          getPartitionWeights(countNeighbours, getOutDegree());
        double firstMax = Integer.MIN_VALUE;
        double secondMax = Integer.MIN_VALUE;
        int firstK = -1;
        int secondK = -1;
        for (int i = 0; i < k; i++) {
          if (partitionWeights[i] > firstMax) {
            secondMax = firstMax;
            firstMax = partitionWeights[i];
            secondK = firstK;
            firstK = i;
          } else if (partitionWeights[i] > secondMax) {
            secondMax = partitionWeights[i];
            secondK = i;
          }
        }
        if (firstMax == secondMax) {
          if (currentPartition != firstK && currentPartition != secondK) {
            desiredPartition = firstK;
          } else {
            desiredPartition = currentPartition;
          }
        } else {
          desiredPartition = firstK;
        }
      }
      return desiredPartition;
    }

    /**
     * Counts the partitions in the neighborhood of the vertex
     *
     * @param messages all recieved messages
     * @return array with all partitions in the neighborhood and counted how
     * often they are there
     */
    private long[] getPartitionFrequencies(final Iterable<Long> messages) {
      long[] result = new long[k];
      for (Long message : messages) {
        result[message.intValue()]++;
      }
      return result;
    }

    /**
     * calculates the partition weights. The node will try to migrate into
     * the partition with the highest weight.
     *
     * @param partitionFrequencies array with all neighbor partitions
     * @param numEdges             total number of edges of this vertex
     * @return calculated partition weights
     */
    private double[] getPartitionWeights(long[] partitionFrequencies,
      long numEdges) {
      double[] partitionWeights = new double[k];
      for (int i = 0; i < k; i++) {
        String capacity_aggregator = CAPACITY_AGGREGATOR_PREFIX + i;
        LongValue aggregatedValue =
          getPreviousIterationAggregate(capacity_aggregator);
        long load = aggregatedValue.getValue();
        long freq = partitionFrequencies[i];
        double weight = (double) freq / (load * numEdges);
        partitionWeights[i] = weight;
      }
      return partitionWeights;
    }

    /**
     * Moves a vertex from its old to its new partition.
     *
     * @param vertex           vertex
     * @param desiredPartition partition to move vertex to
     */
    private void migrateVertex(final Vertex<Long, ARPVertexValue> vertex,
      long desiredPartition) {
      // decrease capacity in old partition
      String oldPartition =
        CAPACITY_AGGREGATOR_PREFIX + vertex.getValue().getCurrentPartition();
      notifyAggregator(oldPartition, NEGATIVE_ONE);
      // increase capacity in new partition
      String newPartition = CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
      notifyAggregator(newPartition, POSITIVE_ONE);
      setNewVertexValue(new ARPVertexValue(desiredPartition,
        vertex.getValue().getDesiredPartition()));
    }

    /**
     * Decides of a vertex is allowed to migrate to a given desired partition.
     * This is based on the free space in the partition and the demand for that
     * partition.
     *
     * @param desiredPartition desired partition
     * @return true if the vertex is allowed to migrate, false otherwise
     */
    private boolean doMigrate(long desiredPartition) {
      long totalCapacity = getTotalCapacity();
      String capacity_aggregator =
        CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
      long load = getAggregatedValue(capacity_aggregator);
      long availability = totalCapacity - load;
      String demand_aggregator = DEMAND_AGGREGATOR_PREFIX + desiredPartition;
      long demand = getAggregatedValue(demand_aggregator);
      double threshold = (double) availability / demand;
      double randomRange = random.nextDouble();
      return Double.compare(randomRange, threshold) < 0;
    }

    /**
     * Returns the total number of vertices a partition can store. This depends
     * on the strict capacity and the capacity threshold.
     *
     * @return total capacity of a partition
     */
    private int getTotalCapacity() {
      double strictCapacity = getNumberOfVertices() / (double) k;
      double buffer = strictCapacity * capacityThreshold;
      return (int) Math.ceil(strictCapacity + buffer);
    }

    /**
     * Return the aggregated value of the previous super-step
     *
     * @param agg aggregator name
     * @return aggregated value
     */
    private long getAggregatedValue(String agg) {
      LongValue aggregatedValue = getPreviousIterationAggregate(agg);
      return aggregatedValue.getValue();
    }
  }

  /**
   * Distributes the partition of the vertex
   */
  public static final class ARPMessage extends
    MessagingFunction<Long, ARPVertexValue, Long, NullValue> {
    @Override
    public void sendMessages(Vertex<Long, ARPVertexValue> vertex) throws
      Exception {
      if ((getSuperstepNumber() % 2) == 1) {
        sendMessageTo(vertex.getId(), vertex.getValue().getCurrentPartition());
      } else {
        sendMessageToAllNeighbors(vertex.getValue().getCurrentPartition());
      }
    }
  }
}

