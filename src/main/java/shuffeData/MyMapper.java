package shuffeData;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	Text v=new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String []values=parserLogger.parse(value.toString());
		if(values[2].startsWith("GET /static")) return ;
		if(values[2].startsWith("GET /")){
			values[2]=values[2].substring("GET /".length());
		}
		if(values[2].startsWith("POST /")){
			values[2]=values[2].substring("POST /".length());
		}
		if(values[2].endsWith("HTTP/1.1")){
			values[2].substring(0,values[2].length()-"HTTP/1.1".length());
		}
		v.set(values[0]+"\t"+values[1]+"\t"+values[2]+"\t"+values[3]+"\t"+values[4]);
		context.write(key, v);
	}
}
