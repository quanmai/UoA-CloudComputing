/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * This is an example Hadoop Map/Reduce application.
 *
 * It inputs a map in adjacency list format, and performs Page Rank Algorithm.
 * The input format is
 * ID   EDGES|RANK
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * RANK = determines the probability (rank) of the node
 **/

public class PageRank extends Configured implements Tool {
  public static int N;
  public static double alpha;
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.GraphSearch");

  public static class MapperPass1 extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable  key, Text value, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {

    //  LOG.info("Map executing for key [" + key.toString() + "] and value [" + value.toString()
    //      + "]");

      Node node = new Node(value.toString());
      double w = node.getWeights()/Double.valueOf(node.numEdges());

      // Emit egdes ID \tab -1|w
      // Handle dangling node, edge = NULL, connecting to a virtual node (id = -1)
      if (node.isDangling()) {
        Node dnode = new Node(-1);
        dnode.setWeights(node.getWeights());
        output.collect(new IntWritable(dnode.getId()), dnode.getLine(dnode.getWeights()));
      }
      // Normal nodes, ID = edge, edge = -1
      else {
        List<Integer> edges = new ArrayList<Integer>();
        edges.add(-1);
        for (int v : node.getEdges()) {
          Node vnode = new Node(v);
          vnode.setEdges(edges);
          vnode.setWeights(w);
          output.collect(new IntWritable(vnode.getId()), vnode.getLine(vnode.getWeights()));
        }
      }
      // No matter what, we emit the input node with Rank=0:  ID \tab edges | 0
      output.collect(new IntWritable(node.getId()), node.getLine(0.0));
    }
  }

  public static class ReducePass1 extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    /**
     * Make a new node which combines all information for this single node id.
     * The new node should have
     * - The full list of edges
     * - The rank
     */
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      
      // LOG.info("Reduce executing for input key [" + key.toString() + "]");
      List<Integer> edges = null;
      double rank = 0.0;

      while (values.hasNext()) {
        Text value = values.next();
        LOG.info("Reduce executing for input values [" + value.toString() + "]");
        Node u = new Node(key.get() + "\t" + value.toString());

        // Normal nodes
        if (u.getEdges().size() > 0) { //Edge = -1 (NULL) also size() > 0
          if (u.numEdges() > -1) {
            edges = u.getEdges();
            // LOG.info("Edges is [" + edges.toString() + "]");
            // LOG.info("Edges size is [" + String.valueOf(u.numEdges()) + "]");
            // LOG.info("First edge is [" + String.valueOf(edges.get(0)) + "]");
          }
        }  
        rank += u.getWeights();
      }

      // dangling node
      if (edges == null) {
        edges = new ArrayList<Integer>();
        edges.add(-1);
      }

      Node n = new Node(key.get());
      n.setEdges(edges);
      n.setWeights(rank);
      output.collect(key, new Text(n.getLine(rank)));
     // LOG.info("Reduce outputting final key [" + key + "] and value [" + n.getLine() + "]");
    }
  }

  public static class MapperPass2 extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {
  /**
   * Pass 2 mapper
   * normal nodes: reproduce themselves
   * dangling node:
   */      
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {

      // LOG.info("Map executing for key [" + key.toString() + "] and value [" + value.toString()+ "]");
      Node node = new Node(value.toString());

      // If the node is a Virtual node (id = -1), 
      if (node.isVirtual()) {
        List <Integer> edges = new ArrayList<Integer>();
        edges.add(-1);
        //DANGLING NODE SHOULD BE HANDLED
        double drank = node.getWeights() / N;
        
        for (int v = 1; v <= N; v++) {
          Node vnode = new Node(v);
          vnode.setEdges(edges);
          vnode.setWeights(drank);
          output.collect(new IntWritable(vnode.getId()), vnode.getLine(vnode.getWeights()));
        }
      }
      else {
        // Normal nodes reproduce themselves with rank=0?
        output.collect(new IntWritable(node.getId()), node.getLine(node.getWeights()));
      }
    }
  }

  public static class ReducePass2 extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    /**
     * Make a new node which combines all information for this single node id.
     * The new node should have
     * - The full list of edges
     * - The weight
     */
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      
      // LOG.info("ReducePass2 executing for input key [" + key.toString() + "]");
      List<Integer> edges = null;
      double rank = 0.0;

      while (values.hasNext()) {
        Text value = values.next();
        LOG.info("ReducePass2 executing for input values [" + value.toString() + "]");
        Node u = new Node(key.get() + "\t" + value.toString());

        // Get node edges, if node is dangling, edge = NULL
        if (u.getEdges().size() > 0) {
           if (u.getEdges().get(0) > -1) 
            edges = u.getEdges();
        }

        rank += u.getWeights();
      }
      if (edges == null) {
        edges = new ArrayList<Integer>();
        edges.add(-1);
      }
    
      rank = (1-alpha) * rank + alpha/N;

      Node n = new Node(key.get());
      n.setEdges(edges);
      n.setWeights(rank);
      if (key.get() != -1)
        output.collect(key, new Text(n.getLine(rank)));
    }
  } 
  static int printUsage() {
    System.out.println("pagerank [-m <num mappers>] [-r <num reducers>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  private JobConf getJobConf1(String[] args) {
    JobConf conf = new JobConf(getConf(), PageRank.class);
    conf.setJobName("pass1");

    // the keys are the unique identifiers for a Node (ints in this case).
    conf.setOutputKeyClass(IntWritable.class);
    // the values are the string representation of a Node
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapperPass1.class);
    conf.setReducerClass(ReducePass1.class);

    //conf.setNumReduceTasks(Integer.parseInt(args[0]));

    for (int i = 0; i < args.length; ++i) {
      if ("-r".equals(args[i])) {
        conf.setNumReduceTasks(Integer.parseInt(args[++i]));
      }
    }

    //LOG.info("The number of reduce tasks has been set to " + conf.getNumReduceTasks());
    //LOG.info("The number of mapper tasks has been set to " + conf.getNumMapTasks());

    return conf; //.waitForCompletion(true);
  }

  private JobConf getJobConf2(String[] args) {
    JobConf conf = new JobConf(getConf(), PageRank.class);
    conf.setJobName("pass2");

    // the keys are the unique identifiers for a Node (ints in this case).
    conf.setOutputKeyClass(IntWritable.class);
    // the values are the string representation of a Node
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapperPass2.class);
    conf.setReducerClass(ReducePass2.class);

    for (int i = 0; i < args.length; ++i) {
      if ("-r".equals(args[i])) {
        conf.setNumReduceTasks(Integer.parseInt(args[++i]));
      } 
    }

    return conf;
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   *
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  public int run(String[] args) throws Exception {

    int totalCount=4; // dummy value

    for (int i = 0; i < args.length; ++i) {
      if ("-i".equals(args[i])) {
        totalCount= Integer.parseInt(args[++i]);
      } else if ("-a".equals(args[i])) {
        alpha = Double.parseDouble(args[++i]);
      } else if ("-n".equals(args[i])) {
        N = Integer.parseInt(args[++i]);
      }
    }
    int iterationCount = 0;

    while (keepGoing(iterationCount,totalCount)) {
      String inputPass1;
      if (iterationCount == 0)
        inputPass1 = "input";
      else
        inputPass1 = "output-" + (iterationCount) + "-2";

      String outputPass1 = "output-" + (iterationCount + 1) + "-1";
      String inputPass2 = outputPass1;
      String outputPass2 = "output-" + (iterationCount + 1) + "-2";

      JobConf conf1 = getJobConf1(args);
      JobConf conf2 = getJobConf2(args);
      FileInputFormat.setInputPaths(conf1, new Path(inputPass1));
      FileOutputFormat.setOutputPath(conf1, new Path(outputPass1));
      FileInputFormat.setInputPaths(conf2, new Path(inputPass2));
      FileOutputFormat.setOutputPath(conf2, new Path(outputPass2));
      RunningJob job1 = JobClient.runJob(conf1);
      RunningJob job2 = JobClient.runJob(conf2);
      iterationCount++;
    }

    return 0;
  }

  private boolean keepGoing(int iterationCount, int totalCount) {
    if(iterationCount >= totalCount) {
      return false;
    }

    return true;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PageRank(), args);
    System.exit(res);
  }

}
