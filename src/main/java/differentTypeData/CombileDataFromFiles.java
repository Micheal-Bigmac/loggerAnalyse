import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombileDataFromFiles extends Configured implements Tool {

	enum Counter {
		TimeSkpip, OutofTimestamp, LineSkip, UserSkip
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		String date;
		String[] timeRange;
		boolean dataSource;

		TableLine line = new TableLine();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			this.date = context.getConfiguration().get("date");
			this.timeRange = context.getConfiguration().get("timeRange")
					.split("-");
			FileSplit fs = (FileSplit) context.getInputSplit();
			String fileName = fs.getPath().getName();// 得到文件名称
			if (fileName.startsWith("POS")) {
				this.dataSource = true;
			} else if (fileName.startsWith("NET")) {
				this.dataSource = false;
			} else {
				throw new IOException("文件名称不对，请指定正确的文件");
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				line.set(value.toString(), this.dataSource, this.date,
						this.timeRange);
			} catch (LineException e) {
				if (e.getFlag() == -1) {
					context.getCounter(Counter.OutofTimestamp).increment(1);
				} else {
					context.getCounter(Counter.TimeSkpip).increment(1);
				}
				return;
			}catch (Exception e) {
				context.getCounter(Counter.LineSkip).increment(1);
				return ;
			}
			context.write(line.outKey(), line.outValue());
		}
	}
	
	public static class reduce extends Reducer<Text, Text, NullWritable, Text>{
		String date;
		SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String first;
		String timeFlag;
		@Override
		protected void setup(
				Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.date=context.getConfiguration().get("date");
		}
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, NullWritable, Text>.Context arg2)
				throws IOException, InterruptedException {
			first=arg0.toString().split("\\|")[0];
			timeFlag=arg0.toString().split("\\|")[1];
			
			String value;
			TreeMap<Long, String> tree=new TreeMap<Long, String>();
			for(Text temp : arg1){
				value=temp.toString();
				tree.put(Long.valueOf(value.split("\\|")[1]), value.split("\\|")[0]);
			}
			try {
				Date tmp = (Date) dateFormat.parseObject(this.date+" "+timeFlag.split("-")[1]+":00:00");
				tree.put(tmp.getTime()/1000L, "OFF");
				HashMap<String, Float> stayTime=getStayTime(tree);
				
				for(Entry<String, Float> entry : stayTime.entrySet()){
					StringBuilder builder = new StringBuilder();
					builder.append(first).append("|");
					builder.append(entry.getKey()).append("|");
					builder.append(timeFlag).append("|");
					builder.append(entry.getValue());
					
					arg2.write( NullWritable.get(), new Text(builder.toString()) );
				}
				
			} catch (ParseException e) {
				arg2.getCounter(Counter.TimeSkpip).increment(1);
				return;
			}catch (Exception e) {
				arg2.getCounter(Counter.UserSkip).increment(1);
				return;
			}
		}
		
		private HashMap<String, Float> getStayTime(TreeMap<Long, String> tree){
			HashMap<String, Float> loc=new HashMap<String, Float>();
			Iterator<Entry<Long, String>> entries=tree.entrySet().iterator();
			Entry<Long, String> next ,nextNext;
			next=entries.next();
			
			while(entries.hasNext()){
				nextNext=entries.next();
				float off =(nextNext.getKey()-next.getKey())/60f;
				if(off<60f){
					if(loc.containsKey(next.getValue())){
						loc.put(next.getValue(), loc.get(next.getValue())+off);
					}else{
						loc.put(next.getValue(), off);
					}
				}
				next=nextNext;
			}
			return loc;
		}
	}
	
	static String input="hdfs://192.168.121.200:9000/Data";
	static String output="hdfs://192.168.121.200:9000/Data/Out";

	public static void main(String[] args) throws Exception {
	

		/*SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		System.out.println(format.parse("2014-3-12 24:34:12"));*/
		
		ToolRunner.run(new CombileDataFromFiles(), args);
		
	}


	public int run(String[] arg0) throws Exception {
		input=arg0[0];
		output=arg0[1];
		Configuration configuration=getConf();
		
		configuration.set("date", arg0[2]);
		configuration.set("timeRange", arg0[3]);
		
		FileSystem fileSystem=FileSystem.get(new URI(input),configuration);	
		Path out=new Path(output);
		if(fileSystem.exists(out)){
			fileSystem.delete(out,true);
		}
		
		Job job=new Job(configuration, CombileDataFromFiles.class.getSimpleName());
		job.setJarByClass(CombileDataFromFiles.class);
		
		
		FileInputFormat.setInputPaths(job, input);
		
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(reduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, out);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
