package mapreduce;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

		Job job = Job.getInstance(conf, "matrix");

		job.setJarByClass(PageRank.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setReducerClass(IdentityReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MatrixMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		int N;
		double B;
		int K;
		double[][] BM;
		double[] C;
		double[] V;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			N = Integer.parseInt(conf.get("N"));
			B = Double.parseDouble(conf.get("B"));
			K = Integer.parseInt(conf.get("K"));
			BM = new double[N][N];
			C = new double[N];
			V = new double[N];
			for (int i = 0; i < N; i++) {
				C[i] = (1 - B) / N;
				V[i] = 1.0 / N;
			}
		}

		@Override
		public void map(Object key, Text value, Context context) {
			String line = value.toString();
			String[] pagePages = line.split(":");
			int y = pagePages[0].charAt(0) - 'A';
			if(pagePages.length < 2){
				return;
			}
			String[] pages = pagePages[1].split(" ");
			int n = pages.length;
			double mv = B * 1 / (n - 1);
			for (String str : pages) {
				if(str.equals(""))
					continue;
				int x = str.charAt(0) - 'A';
				BM[x][y] = mv;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			while (K > 0) {
				K--;
				double[] Vt = new double[N];
				for (int i = 0; i < N; i++) {
					for (int j = 0; j < N; j++) {
						Vt[i] += BM[i][j] * V[j];
					}
					Vt[i] += C[i];
				}
				for (int i = 0; i < N; i++){
					V[i] = Vt[i];
				}
			}
			for (int i = 0; i < N; i++) {
				context.write(new IntWritable(i), new DoubleWritable(V[i]));
			}
		}
	}

	public static class IdentityReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		@Override
		public void reduce(IntWritable key, DoubleWritable values, Context context) throws IOException, InterruptedException {
			context.write(key, values);
		}
	}
}
