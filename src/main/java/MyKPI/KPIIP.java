package MyKPI;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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



public class KPIIP {

	public static class KPIIPMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			KPI kpi=KPI.filterIPs(value.toString());
			if(kpi.isValid()){
				context.write(new Text(kpi.getRequest()), new Text(kpi.getRemote_addr()));
			}
		}
	}
	public static class KPIReducer extends Reducer<Text, Text, Text, IntWritable>{
		private Set<String> set=new HashSet<String>();
			@Override
			protected void reduce(Text arg0, Iterable<Text> arg1,
					Reducer<Text, Text, Text, IntWritable>.Context arg2)
					throws IOException, InterruptedException {
				for(Text temp : arg1){
					set.add(temp.toString());
				}
				arg2.write(arg0, new IntWritable(set.size()));
			}
	}
	static String input="hdfs://192.168.121.200:9000/2-6";
	static String output="hdfs://192.168.121.200:9000/2-6/ip";
	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		FileSystem fileSystem=FileSystem.get(new URI(input), configuration);
		
		Path out=new Path(output);
		if(fileSystem.exists(out)){
			fileSystem.delete(out, true);
		}
		
		Job job=new Job(configuration,KPIIP.class.getSimpleName());
		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(KPIIPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(KPIReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, out);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

	}

}
