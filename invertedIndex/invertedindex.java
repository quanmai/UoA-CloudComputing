mport java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class invertedindex{
    public static final int numPartitions = 3;
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, wordpair, IntWritable> {
        HashMap count = new HashMap<wordpair, Integer>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            int sum = 0;
            while (itr.hasMoreTokens()) {
                wordpair wordPair = new wordpair();
                wordPair.setWord(itr.nextToken());
                wordPair.setFile(fileName);
                //System.out.println(wordPair);
                //System.out.println(count);
                if (count.containsKey(wordPair)) {
                    sum = (int) count.get(wordPair) + 1;
                    count.put(wordPair, sum);
                }
                else {
                    count.put(wordPair, 1);
                }
                //System.out.println("----------------------------------------count--------------------------------------");
                //System.out.println(count);
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<wordpair, Integer>> temp = count.entrySet().iterator();
            while(temp.hasNext()) {
                Map.Entry<wordpair, Integer> entry = temp.next();
                String keyVal = entry.getKey().toString();
                Integer countVal = entry.getValue();

                context.write(new wordpair(keyVal), new IntWritable(countVal));
            }
        }
    }
    public static class FullKeyComparator extends WritableComparator {

        public FullKeyComparator() {
            super(wordpair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
             wordpair key1 = (wordpair) wc1;
             wordpair key2 = (wordpair) wc2;
             int val = key1.getWord().compareTo(key2.getWord());
             if (val!=0) {
                return val;
             }else return key1.getFile().compareTo(key2.getFile());
        }
    }
    public static class WordPairPartitioner extends Partitioner<wordpair,IntWritable> {
        @Override
        public int getPartition(wordpair wordPair, IntWritable intWritable, int numPartitions) {
            return (wordPair.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    public static class InvertedIndexReducer extends Reducer<wordpair, IntWritable, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(wordpair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Text term = new Text(key.getWord().toString());
            Text file = new Text(key.getFile().toString());

            StringBuilder docValueList = new StringBuilder();
            for(IntWritable val : values){
                docValueList.append(key.getFile().toString() + ":" + val + ";");
            }

            context.write(term, new Text(docValueList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
          System.err.println("Usage: invertedindex <in> <out>");
          System.exit(2);
        }
        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(invertedindex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        //job.setSortComparatorClass(FullKeyComparator.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(wordpair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(numPartitions);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
