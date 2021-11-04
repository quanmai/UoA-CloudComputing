import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
public class Node {
  //ID  EDGES|WEIGHTS|DISTANCE|COLOR
  public static enum Color {
    WHITE, GRAY, BLACK
  };
  private final int id;
  private int distance;
  private List<Integer> edges = new ArrayList<Integer>();
  private List<Integer> weights = new ArrayList<Integer>();
  private Color color = Color.WHITE;
  public Node(String str) {
    String[] map = str.split("\t");
    String key = map[0];
    String value = map[1];
    String[] tokens = value.split("\\|");
    this.id = Integer.parseInt(key);
    for (String s : tokens[0].split(",")) {
      if (s.length() > 0) {
        edges.add(Integer.parseInt(s));
      }
    }
    for (String s : tokens[1].split(",")) {
      if (s.length() > 0) {
        weights.add(Integer.parseInt(s));
      }
    }
    if (tokens[2].equals("Integer.MAX_VALUE")) {
      this.distance = Integer.MAX_VALUE;
    } else {
      this.distance = Integer.parseInt(tokens[2]);
    }
System.out.println(tokens[2]);
    this.color = Color.valueOf(tokens[3]);
  }
  public Node(int id) {
    this.id = id;
  }
  public int getId() {
    return this.id;
  }
  public int getDistance() {
    return this.distance;
  }
  public void setDistance(int distance) {
    this.distance = distance;
  }
  public Color getColor() {
    return this.color;
  }
  public void setColor(Color color) {
    this.color = color;
  }
  public List<Integer> getEdges() {
    return this.edges;
  }
  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }
  public List<Integer> getWeights() {
    return this.weights;
  }
  public void setWeights(List<Integer> weights) {
    this.weights = weights;
  }
  public Text getLine() {
    StringBuffer s = new StringBuffer();
    for (int v : edges) {
      s.append(v).append(",");
    }
    s.append("|");
    for (int w : weights) {
      s.append(w).append(",");
    }
    s.append("|");
    if (this.distance < Integer.MAX_VALUE) {
      s.append(this.distance).append("|");
    } else {
      s.append("Integer.MAX_VALUE").append("|");
    }
    s.append(color.toString());
    return new Text(s.toString());
  }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
