package bigdataLab1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilteredMain {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String samplepath = args[1];
		String outputpath = args[2];
		ArrayList<Double> ratelist = new ArrayList<Double>();
		Path path = new Path(samplepath);
		try {
		FileSystem inputPathFs= FileSystem.get(URI.create(samplepath),conf);
		if(inputPathFs.exists(path)) {
			String charset = "UTF-8";
			FSDataInputStream fsDataInputStream = inputPathFs.open(path);
			InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream,charset);
			String  line =null;
			BufferedReader reader = null;
			reader  = new BufferedReader(inputStreamReader);
			while((line=reader.readLine())!=null) {
				String[] ttt=line.toString().split("\\|");
			//	System.out.println(ttt[0] + "\t" + ttt[1]);
				if(!ttt[6].equals("?"))
				ratelist.add(Double.valueOf(ttt[6]));
			}
		}}catch(IOException e) {
			e.printStackTrace();
		}
		Collections.sort(ratelist, new Comparator<Double>() {
			@Override
			public int compare (Double o1,Double o2) {	
				return o1.compareTo(o2);
			}
		});//xiao dao da
		int n1=ratelist.size()/100;
		int n2=ratelist.size()-(ratelist.size()/100);
		conf.setDouble("first",ratelist.get(n1));
		conf.setDouble("last",ratelist.get(n2));
		System.out.println(ratelist.get(n1));
		System.out.println(ratelist.get(n2));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputpath), true);
		Job job = Job.getInstance(conf, "Filtered");
	    job.setJarByClass(FilteredMain.class);
	    job.setMapperClass(FilteredMapper.class);
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(FilteredReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputpath));
	    FileOutputFormat.setOutputPath(job,new Path(outputpath));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
