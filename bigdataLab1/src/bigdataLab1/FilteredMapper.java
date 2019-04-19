package bigdataLab1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilteredMapper extends Mapper<LongWritable,Text,Text,Text>{
	private double first=0;
	private double last=0;
	protected void  setup(Context context) throws IOException,InterruptedException{
		super.setup(context);
		Configuration conf = context.getConfiguration();
	    first=conf.getDouble("first", 60.00);
	    last=conf.getDouble("last", 99.00);
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] ttt=value.toString().split("\\|");
		//System.out.println(ttt[1]);
		//System.out.println(first+"\t"+last);
		//System.out.println(Double.valueOf(ttt[1])>8.1461295 && Double.valueOf(ttt[1])<11.1993265);
		if(!ttt[6].equals("?")) {
		if(Double.valueOf(ttt[1])>8.1461295 && Double.valueOf(ttt[1])<11.1993265 &&
		Double.valueOf(ttt[2])>56.5824856 && Double.valueOf(ttt[2])<57.750511 && 
		Double.valueOf(ttt[6])>first && Double.valueOf(ttt[6])<last)//
		context.write(new Text(ttt[10]),value);
		}
		else {
			if(Double.valueOf(ttt[1])>8.1461295 && Double.valueOf(ttt[1])<11.1993265 &&
					Double.valueOf(ttt[2])>56.5824856 && Double.valueOf(ttt[2])<57.750511 )
			context.write(new Text(ttt[10]),value);
		}
	}
}
