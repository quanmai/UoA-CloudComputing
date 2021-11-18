import java.util.*;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

public class Node {

  private final int id;
  private List<Integer> edges = new ArrayList<Integer>();
  private double weights;

  public Node(String str) {

    String[] map = str.split("\\s+");
    String key = map[0];
    String value = map[1];

    String[] tokens = value.split("\\|");
    this.id = Integer.parseInt(key);
    
    if (tokens[0].equals("NULL"))  edges.add(-1);
    else {
      for (String s : tokens[0].split(",")) {
        if (s.length() > 0) {
          edges.add(Integer.parseInt(s));
        }
      }
    }

    this.weights = Double.parseDouble(tokens[1]);
  }

  public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }

  public int numEdges() {
    if (this.edges.get(0) == -1) return -1;
    else return this.edges.size();
  }

  public double getWeights() {
    return this.weights;
  }

  public void setWeights(double weights) {
    this.weights = weights;
  }

  public boolean isDangling() {
    return (this.numEdges() == -1);
  }

  public boolean isVirtual() {
    return (this.id == -1);
  }

  public Text getLine(double w) {
    StringBuffer s = new StringBuffer();
    
    for (int v : edges) {
      if (v > -1) {
        if (s.length()==0) s.append(v);
        else s.append(",").append(v);
      }
      else {
        s.append("NULL");
        break;
      }
    }
    s.append("|");
    s.append(w);	
    return new Text(s.toString());
  }

}
