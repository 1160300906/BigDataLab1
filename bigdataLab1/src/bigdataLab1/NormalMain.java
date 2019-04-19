package bigdataLab1;

//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NormalMain {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
//		String samplepath = args[1];
		String outputpath = args[1];
//		double maxrate=0;
//		double minrate=100;
//		Path path = new Path(samplepath);
//		try {
//		FileSystem inputPathFs= FileSystem.get(URI.create(samplepath),conf);
//		if(inputPathFs.exists(path)) {
//			String charset = "UTF-8";
//			FSDataInputStream fsDataInputStream = inputPathFs.open(path);
//			InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream,charset);
//			String  line =null;
//			BufferedReader reader = null;
//			reader  = new BufferedReader(inputStreamReader);
//			while((line=reader.readLine())!=null) {
//				String[] ttt=line.toString().split("\\|");
//				if(!ttt[6].equals("?")) {
//			//	ratelist.add(Double.valueOf(ttt[6]));
//				if(Double.valueOf(ttt[6])>maxrate)
//						maxrate=Double.valueOf(ttt[6]);
//				if(Double.valueOf(ttt[6])<minrate)
//					minrate=Double.valueOf(ttt[6]);
//				}
//			}
//		}}catch(IOException e) {
//			e.printStackTrace();
//		}
//		conf.setDouble("min",minrate);
//		conf.setDouble("max",maxrate);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputpath), true);
		Job job = Job.getInstance(conf, "Normal");
	    job.setJarByClass(NormalMain.class);
	    job.setMapperClass(NormalMapper.class);
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(NormalReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputpath));
	    FileOutputFormat.setOutputPath(job,new Path(outputpath));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
