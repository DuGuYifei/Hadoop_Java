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
//import java.util.StringTokenizer;

public class FilterCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FilterCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "FilterCount");

        job.setJarByClass(FilterCount.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FilterMap.class);
        job.setReducerClass(CountReduce.class);

        job.setOutputKeyClass(VisitorTuple.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(VisitorTuple.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FilterMap extends Mapper<Object, Text, VisitorTuple, IntWritable> {
        IntWritable intWrite = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {			
            String line = value.toString();
            String[] params = line.split(",");
            if(params[6] == "" || params[11] == "" || params[6].contains(".") || params[11].contains("."))
                return;

            VisitorTuple visitoree = new VisitorTuple();
            visitoree.setLastName(params[0]);
            visitoree.setFirstName(params[1]);
            visitoree.setMidName(params[2]);
            context.write(visitoree, intWrite);
			// String line = value.toString();
            // StringTokenizer tokenizer = new StringTokenizer(line, ",");
            // if(!tokenizer.hasMoreTokens())
            //     return;
            // VisitorTuple visitor = new VisitorTuple();
            // String lastName = tokenizer.nextToken();
            // visitor.setLastName(lastName);
            // String firstName = tokenizer.nextToken();
            // visitor.setFirstName(firstName);
            // String midName = tokenizer.nextToken();
            // visitor.setMidName(midName);

            // for(int id = 3; id < 12; id++){
            //     if(!tokenizer.hasMoreTokens())
            //         return;
            //     String nextToken = tokenizer.nextToken();
            //     if(id == 6 && nextToken == "")
            //         return;
            //     if(id == 11 && nextToken == "" )
            //         return;
            // }
            // context.write(visitor, intWrite);
		}
    }

    public static class CountReduce extends Reducer<VisitorTuple, IntWritable, VisitorTuple, IntWritable> {
        @Override
        public void reduce(VisitorTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
        }
    }
}