import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);
        String temporaryPath = conf.get("tmpPath");
        Path tmpPath = new Path(temporaryPath);
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Popular Links Count");
        Job jobB = Job.getInstance(conf, "Top Popular Links");
        jobA.setJarByClass(TopPopularLinks.class);
        jobB.setJarByClass(TopPopularLinks.class);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.waitForCompletion(true);

        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        jobB.setNumReduceTasks(1);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final IntWritable outKey = new IntWritable();
        private final IntWritable outValue = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            boolean firstPg = true;
            while (tokenizer.hasMoreTokens()) {
                int nextToken = Integer.parseInt(tokenizer.nextToken());
                if (firstPg) {
                    firstPg = false;
                } else {
                    outKey.set(nextToken);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private Integer N;
        private TreeSet<ComparablePair<Integer, Integer>> countToIdMap = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countToIdMap.add(new ComparablePair<>(sum, key.get()));
            if (countToIdMap.size() > N) {
                countToIdMap.remove(countToIdMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ComparablePair<Integer, Integer> item : countToIdMap) {
                context.write(new IntWritable(item.getValue()), new IntWritable(item.getKey()));
            }
        }

    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int id = Integer.parseInt(key.toString());
            int count = Integer.parseInt(value.toString());
            Integer[] pair = {id, count};
            IntArrayWritable val = new IntArrayWritable(pair);
            context.write(NullWritable.get(), val);
        }

    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private Integer N;
        private TreeSet<ComparablePair<Integer, Integer>> countToIdMap = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable  val: values) {
                IntWritable[] pair = (IntWritable[]) val.toArray();
                Integer id = pair[0].get();
                Integer count = pair[1].get();
                countToIdMap.add(new ComparablePair<Integer, Integer>(count, id));
            }
            if (countToIdMap.size() > N) {
                countToIdMap.remove(countToIdMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ComparablePair<Integer, Integer> item : countToIdMap) {
                context.write(new IntWritable(item.getValue()), new IntWritable(item.getKey()));
            }
        }
    }
}

// >>> Don't Change

class ComparablePair<K extends Comparable<? super K>, V extends Comparable<? super V>>
        extends javafx.util.Pair<K,V>
        implements Comparable<ComparablePair<K, V>> {

    public ComparablePair(K key, V value) {
        super(key, value);
    }

    @Override
    public int compareTo(ComparablePair<K, V> o) {
        int cmp = o == null ? 1 : (this.getKey()).compareTo(o.getKey());
        return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
    }

}

// <<< Don't Change