package algorithms.labelpropagation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.types.NullValue;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Helper Class for LPTest
 */
public class LPComputationTestHelper {
  private static final Pattern SEPARATOR = Pattern.compile(" ");

  public static Graph<Long, LPVertexValue, NullValue> getGraph(String[] graph,
    ExecutionEnvironment env) {
    List<Vertex<Long, LPVertexValue>> vertices = Lists.newArrayList();
    List<Edge<Long, NullValue>> edges = Lists.newArrayList();
    for (String line : graph) {
      String[] tokens = SEPARATOR.split(line);
      long id = Long.parseLong(tokens[0]);
      long value = Long.parseLong(tokens[1]);
      vertices.add(new Vertex<>(id, new LPVertexValue(id, value)));
      for (int n = 2; n < tokens.length; n++) {
        long tar = Long.parseLong(tokens[n]);
        edges.add(new Edge<>(id, tar, NullValue.getInstance()));
      }
    }
    return Graph.fromCollection(vertices, edges, env);
  }
}
