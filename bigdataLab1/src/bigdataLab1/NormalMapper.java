package bigdataLab1;

import java.io.IOException;

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.*;
public class NormalMapper extends Mapper<LongWritable,Text,Text,Text>{
//	private double min=0;
//	private double max=0;
//	protected void  setup(Context context) throws IOException,InterruptedException{
//		super.setup(context);
//		Configuration conf = context.getConfiguration();
//	    min=conf.getDouble("min", 60.00);
//	    max=conf.getDouble("max", 99.00);
//	}
	private static String normaltime(String date) {
        String timepattern1="(\\d+)\\/(\\d+)\\/(\\d+)";
        String timepattern2="([A-Z][a-z]+)\\s(\\d+),(\\d+)";
		 Pattern r1 = Pattern.compile(timepattern1);
	        Matcher m1 = r1.matcher(date);
	        Pattern r2 = Pattern.compile(timepattern2);
	        Matcher m2 = r2.matcher(date);
	        if(m1.find())
	        	return m1.group(1)+"-"+m1.group(2)+"-"+m1.group(3);
	        else if(m2.find()) {
	        	String month="";
	        	if(m2.group(1).equals("January"))
	        		month="01";
	        	else if(m2.group(1).equals("February"))
	        		month="02";
	        	else if(m2.group(1).equals("March"))
	        		month="03";
	        	else if(m2.group(1).equals("April"))
	        		month="04";
	        	else if(m2.group(1).equals("May"))
	        		month="05";
	        	else if(m2.group(1).equals("June"))
	        		month="06";
	        	else if(m2.group(1).equals("July"))
	        		month="07";
	        	else if(m2.group(1).equals("August"))
	        		month="08";
	        	else if(m2.group(1).equals("September"))
	        		month="09";
	        	else if(m2.group(1).equals("October"))
	        		month="10";
	        	else if(m2.group(1).equals("November"))
	        		month="11";
	        	else if(m2.group(1).equals("December"))
	        		month="12";
	        	return m2.group(3)+"-"+month+"-"+m2.group(2);
	        }
	        else
	        	return date;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] ttt=value.toString().split("\\|");
        String aftervalue = "";
        aftervalue=ttt[0]+"|"+ttt[1]+"|"+ttt[2]+"|"+ttt[3]+"|"+normaltime(ttt[4])+"|";
        String pattern = "(-*\\d+\\.*+\\d*)(��)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ttt[5]);
        String pattern1 = "(-*\\d+\\.*+\\d*)(�H)";
        Pattern r1 = Pattern.compile(pattern1);
        Matcher m1= r1.matcher(ttt[5]);
        if(m.find())
		     aftervalue=aftervalue+String.format("%.1f",Double.valueOf(m.group(1))*9/5+32 )+"℉"; 
		else if(m1.find())
		     aftervalue=aftervalue+m1.group(1)+"℉";
        if(ttt[6].equals("?"))
             aftervalue=aftervalue+"|"+ttt[6];
        else
        	  aftervalue= aftervalue+"|"+String.format("%.2f",(Double.valueOf(ttt[6])-59.34)/(95.94-59.34));
            // aftervalue= aftervalue+"|"+String.format("%.2f",(Double.valueOf(ttt[6])-min)/(max-min));
        aftervalue= aftervalue+"|"+ttt[7]+"|"+normaltime(ttt[8])+"|"+ttt[9]+"|"+ttt[10]+"|"+ttt[11];
        context.write(new Text(ttt[10]),new Text(aftervalue));
	}
}
