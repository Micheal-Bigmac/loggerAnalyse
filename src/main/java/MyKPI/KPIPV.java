package MyKPI;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPIPV {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			KPI kp = KPI.filterPVs(value.toString());
			if (kp.isValid()) {
				context.write(new Text(kp.getRequest()), new IntWritable(1));
			}
		}
	}
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable temp : arg1){
				sum+=temp.get();
			}
			arg2.write(arg0, new IntWritable(sum));
		}
	}
	static String input ="hdfs://192.168.121.200:9000/2-6";
	static String output ="hdfs://192.168.121.200:9000/2-6/pv";
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		FileSystem fileSystem=FileSystem.get(new URI(input), conf);
		Path output_path=new Path(output);
		if(fileSystem.exists(output_path)){
			fileSystem.delete(output_path, true);
		}
		
		Job job=new Job(conf,KPIPV.class.getSimpleName());
		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, output_path);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
	}
}
