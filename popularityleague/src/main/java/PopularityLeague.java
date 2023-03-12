import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    // <<< Don't Change

    public static String readHDFSFile(String path, Configuration conf) throws IOException {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while ((line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);

        Job jobA = Job.getInstance(conf, "Popular League");
        jobA.setJarByClass(PopularityLeague.class);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
        jobA.waitForCompletion(true);

        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final IntWritable outKey = new IntWritable();
        private final IntWritable outValue = new IntWritable(1);
        private final List<Integer> league = new ArrayList<>();

        //setup
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            String[] leagueStrList = readHDFSFile(leaguePath, conf).split("\n");
            for(String str: leagueStrList) {
                league.add(Integer.parseInt(str));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            boolean firstPg = true;
            while (tokenizer.hasMoreTokens()) {
                Integer nextToken = Integer.parseInt(tokenizer.nextToken());
                if (firstPg) {
                    firstPg = false;
                } else {
                    if(league.contains(nextToken)) {
                        outKey.set(nextToken);
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private Integer N;
        private TreeMap<Integer, Integer> countToIdMap = new TreeMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 16);
            String leaguePath = conf.get("league");
            String[] leagueStrList = readHDFSFile(leaguePath, conf).split("\n");
            for(String str: leagueStrList) {
                countToIdMap.put(Integer.parseInt(str), 0);
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countToIdMap.put(key.get(), countToIdMap.get(key.get()) + sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Integer> item : countToIdMap.entrySet()) {
                int rank = 0;
                for(Map.Entry<Integer, Integer> item2 : countToIdMap.entrySet()) {
                    if(item2.getValue() < item.getValue())
                        rank++;
                }
                context.write(new IntWritable(item.getKey()), new IntWritable(rank));
            }
        }

    }
}