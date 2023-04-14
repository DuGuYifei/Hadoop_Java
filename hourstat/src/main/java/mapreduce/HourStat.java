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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class HourStat extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HourStat(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf, "FilterCount");

        job.setJarByClass(HourStat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FilterMap.class);
        job.setReducerClass(CountReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FilterMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");

        private long getDateDiff(Date end, Date start) {
            long diffInMillies = end.getTime() - start.getTime();
            TimeUnit tu = TimeUnit.MINUTES;
            return tu.convert(diffInMillies, TimeUnit.MILLISECONDS);
            //return diffInMillies;
        }

        private long getDateDiffFromFields(String endString, String startString) throws ParseException{
            Date appointmentDateTime = frmtIn.parse(endString);
            Date arriveDateTime = frmtIn.parse(startString);
            return getDateDiff(appointmentDateTime, arriveDateTime);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] params = line.split(",");
            int diff = 0;
            try {
                if(params[6] == "" || params[11] == "")
                    return;
                diff = (int)getDateDiffFromFields(params[11], params[6]);
                if(Math.abs(diff) > 24 * 60){
                    return;
                }

            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
            
            Date appointmentDate;
            try {
                appointmentDate = frmtIn.parse(params[11]);
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            } 
            SimpleDateFormat frmtOut = new SimpleDateFormat("HH"); 
            int hour = Integer.parseInt(frmtOut.format(appointmentDate)); 
            context.write(new IntWritable(hour), new IntWritable(diff));
        }
    }

    public static class CountReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double[] arr = new double[6];
            for (IntWritable val : values) {
                arr[0]++;
                if(val.get() > 0){
                    arr[3]++;
                    arr[4] += val.get();
                }else if(val.get() < 0){
                    arr[1]++;
                    arr[2] -= val.get();
                }
                arr[5] += Math.abs(val.get());
            }
            arr[4] /= arr[3];
            arr[2] /= arr[1];
            arr[5] /= arr[0];
            context.write(key, new Text(Arrays.toString(arr)));
        }
    }
}
