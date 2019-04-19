package bigdataLab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SampleMain {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputpath), true);
		Job job = Job.getInstance(conf, "Sample");
	    job.setJarByClass(SampleMain.class);
	    job.setMapperClass(SampleMapper.class);
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(SampleReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputpath));
	    FileOutputFormat.setOutputPath(job,new Path(outputpath));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
