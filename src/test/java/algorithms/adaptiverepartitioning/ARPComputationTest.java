package algorithms.adaptiverepartitioning;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class ARPComputationTest {
  int maxIteration = 100;
  int k = 2;
  double threshold = 0.2;
  int countVerticesPartitionZero = 0;

  public static String[] getConnectedGraphWithVertexValues() {
    return new String[] {
      "0 0 1 2 3", "1 1 0 2 3", "2 2 0 1 3 4", "3 3 0 1 2", "4 4 2 5 6 7",
      "5 5 4 6 7 8", "6 6 4 5 7", "7 7 4 5 6", "8 8 5 9 10 11", "9 9 8 10 11",
      "10 10 8 9 11", "11 11 8 9 10"
    };
  }

  @Test
  public void testSmallConnectedGraph() throws Exception {
    VertexCentricConfiguration parameters = new VertexCentricConfiguration();
    parameters.setName("Gelly Partitioning");
    parameters.setParallelism(1);
    parameters.setOptDegrees(true);
    parameters.setOptNumVertices(true);
    for (int i = 0; i < k; i++) {
      parameters
        .registerAggregator(ARPComputation.CAPACITY_AGGREGATOR_PREFIX + i,
          new LongSumAggregator());
      parameters.registerAggregator(ARPComputation.DEMAND_AGGREGATOR_PREFIX + i,
        new LongSumAggregator());
    }
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    Graph<Long, ARPVertexValue, NullValue> gellyGraph = ARPComputationTestHelper
      .getGraph(getConnectedGraphWithVertexValues(), env);
    DataSet<Vertex<Long, ARPVertexValue>> labeledGraph =
      gellyGraph.run(new ARPComputation(maxIteration, k, threshold, parameters))
        .getVertices();
    validateConnectedGraphResult(parseResult(labeledGraph.collect()));
  }

  private void validateConnectedGraphResult(Map<Long, Long> vertexIDwithValue) {
    int n = vertexIDwithValue.size();
    int optimalPartitionSize = n / k;
    double countedOccupation = (float) countVerticesPartitionZero / n;
    double estimatedOccupation =
      (optimalPartitionSize + (optimalPartitionSize * threshold)) / n;
    assertTrue(Double.compare(countedOccupation, estimatedOccupation) <= 0);
  }

  private Map<Long, Long> parseResult(
    List<Vertex<Long, ARPVertexValue>> graph) {
    Map<Long, Long> result = new HashMap<>();
    for (Vertex<Long, ARPVertexValue> v : graph) {
      result.put(v.getId(), v.getValue().getCurrentPartition());
    }
    return result;
  }
}
