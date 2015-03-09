package shuffeData;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class deal extends Configured implements Tool {
	public static final String input = "hdfs://hadoop-main:9000/deal-log/access_2014_12_20.log";
	public static final String output = "hdfs://hadoop-main:9000/deal-log-clean/2014_12_20";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new deal(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, deal.class.getSimpleName());
		job.setJarByClass(deal.class);

		FileSystem fileSystem = FileSystem.get(new URI(output), conf);
		Path path = new Path(output);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		FileInputFormat.setInputPaths(job, input);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, path);
		job.waitForCompletion(true);

		return 0;
	}
}
