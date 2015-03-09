package shuffeData;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void reduce(LongWritable arg0, Iterable<Text> arg1,
			Reducer<LongWritable, Text, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		for (Text text : arg1) {
			arg2.write(text, NullWritable.get());
		}
	}
}
