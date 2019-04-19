package bigdataLab1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SampleMapper extends Mapper<LongWritable,Text,Text,Text>{
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] ttt=value.toString().split("\\|");
		context.write(new Text(ttt[10]),value);
	}
}
