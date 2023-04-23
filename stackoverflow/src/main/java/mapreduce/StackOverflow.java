package mapreduce;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StackOverflow extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StackOverflow(), args);
		System.exit(res);
	}
	// map: id - isFinished tag answerNum  | java 0 cs 1 php 2 python 3
	// clean: tag-idFinished answerNumTotal questionIdNum  | JAVA_FIN 0 JAVA_UNFIN 1 CS_FIN 2 CS_UNFIN 3 PHP_FIN 4 PHP_UNFIN 5 PYTHON_FIN 6 PYTHON_UNFIN 7
	// reduce: tag-idFinished questionIdNum answerNumTotal/idNum

	public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() {
			super(IntWritable.class);
		}
		public IntArrayWritable(IntWritable[] arr){
			super(IntWritable.class, arr);
		}
	}

	public static class DoubleArrayWritable extends ArrayWritable {
		public DoubleArrayWritable() {
			super(DoubleWritable.class);
		}
		public DoubleArrayWritable(DoubleWritable[] arr){
			super(DoubleWritable.class, arr);
		}

		@Override
		public String toString() {
			String res = "";
			for(DoubleWritable dw : (DoubleWritable[])get()){
				res = res + " " + dw.get();
			}
			return res;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

		Job job = Job.getInstance(conf, "matrix");

		job.setJarByClass(StackOverflow.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PreloadMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

		job.setReducerClass(CalculateReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class PreloadMapper extends Mapper<Object, Text, IntWritable, IntArrayWritable> {
		Map<Integer, int[]> qaMap = new HashMap<>();

		@Override
		public void map(Object key, Text value, Context context) {
			String line = value.toString();
			String[] paramStr = line.split(",");
			int type = Integer.parseInt(paramStr[0]);
			int id = type == 1? Integer.parseInt(paramStr[1]) : Integer.parseInt(paramStr[3]);
			qaMap.putIfAbsent(id, new int[]{0, 0, 0});
			if(qaMap.get(id)[0] == 0 && type == 1 && !paramStr[2].equals("")){
				qaMap.get(id)[0] = 1;
			}
			if (paramStr.length == 6){
				switch(paramStr[5]){
					case "Java":
						qaMap.get(id)[1] = 0;
						break;
					case "C#":
						qaMap.get(id)[1] = 1;
						break;
					case "PHP":
						qaMap.get(id)[1] = 2;
						break;
					case "Python":
						qaMap.get(id)[1] = 3;
						break;
				}
			}
			if(type != 1){
				qaMap.get(id)[2]++;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			int[][] res = new int[8][2];
			qaMap.forEach((k,v)->{
				if(v[1] == 0){
					if(v[0] == 1){
						res[0][1]++;
						res[0][0] += v[2];
					}else{
						res[1][1]++;
						res[1][0] += v[2];
					}
				}else if(v[1] == 1){
					if(v[0] == 1){
						res[2][1]++;
						res[2][0] += v[2];
					}else{
						res[3][1]++;
						res[3][0] += v[2];
					}
				}else if(v[1] == 2){
					if(v[0] == 1){
						res[4][1]++;
						res[4][0] += v[2];
					}else{
						res[5][1]++;
						res[5][0] += v[2];
					}
				}else if(v[1] == 3){
					if(v[0] == 1){
						res[6][1]++;
						res[6][0] += v[2];
					}else{
						res[7][1]++;
						res[7][0] += v[2];
					}
				}
			});
			for(int i = 0; i < 7; i++){
				IntWritable[] arr = new IntWritable[2];
				for(int j = 0; j < 2; j++){
					arr[j] = new IntWritable(res[i][j]);
				}
				context.write(new IntWritable(i), new IntArrayWritable(arr));
			}
		}

	}

	public static class CalculateReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, DoubleArrayWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
			for(IntArrayWritable arr : values){
				DoubleWritable[] dw = new DoubleWritable[2];
				IntWritable[] vs = (IntWritable[])arr.toArray();
				dw[0] = new DoubleWritable(vs[1].get());
				dw[1] = new DoubleWritable((double)vs[0].get() / vs[1].get());
				DoubleArrayWritable aw = new DoubleArrayWritable(dw);
				context.write(key, aw);
			}
		}
		
	}

}