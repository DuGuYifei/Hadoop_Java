package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class CountCombo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountCombo(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "FilterCount");

        job.setJarByClass(CountCombo.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FilterMap.class);
        job.setReducerClass(CountReduce.class);

        job.setOutputKeyClass(VisitoreeTuple.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(VisitoreeTuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FilterMap extends Mapper<Object, Text, VisitoreeTuple, IntWritable> {
        IntWritable intWrite = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] params = line.split(",");
            if(params[6] == "" || params[11] == "" || params[6].contains(".") || params[11].contains("."))
                return;

            VisitoreeTuple visitoree = new VisitoreeTuple();
            visitoree.setLastName(params[0]);
            visitoree.setFirstName(params[1]);
            visitoree.setMidName(params[2]);
            visitoree.setVisitoreeLastName(params[19]);
            visitoree.setVisitoreeFirstName(params[20]);
            context.write(visitoree, intWrite);
        }
    }

    public static class CountReduce extends Reducer<VisitoreeTuple, IntWritable, VisitoreeTuple, IntWritable> {
        TreeMap<Integer, ArrayList<VisitoreeTuple>> rankmap2 = new TreeMap<>();
        HashMap<VisitoreeTuple, Integer> rankmap = new HashMap<>();

        @Override
        public void reduce(VisitoreeTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // VisitoreeTuple nkey = new VisitoreeTuple();
            // nkey.setLastName(key.getLastName());
            // nkey.setFirstName(key.getFirstName());
            // nkey.setMidName(key.getMidName());
            // nkey.setVisitoreeLastName(key.getVisitoreeLastName());
            // nkey.setVisitoreeFirstName(key.getVisitoreeFirstName());

            context.write(key, new IntWritable(sum));

            //rankmap.put(nkey, rankmap.getOrDefault(nkey, 0) + sum);
        }

        // @Override
        // protected void cleanup(Context context) throws IOException, InterruptedException {
        //     for (Map.Entry<VisitoreeTuple, Integer> entry : rankmap.entrySet()) {
        //         context.write(entry.getKey(), new IntWritable(entry.getValue()));
        //         // ArrayList<VisitoreeTuple> list = rankmap2.getOrDefault(entry.getValue(), new ArrayList<>());
        //         // list.add(entry.getKey());
        //         // rankmap2.put(entry.getValue(), list);
        //         // if(rankmap2.size() > 20){
        //         //     rankmap2.remove(rankmap2.firstKey());
        //         // }
        //     }
        //     // for (Map.Entry<Integer, ArrayList<VisitoreeTuple>> entry : rankmap2.entrySet()) {
        //     //     for(VisitoreeTuple visitoree : entry.getValue()){
        //     //         context.write(visitoree, new IntWritable(entry.getKey()));
        //     //     }
        //     // }
        // }

    }
}
