package bigdataLab1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import java.util.ArrayList;
public class SampleReducer extends Reducer<Text, Text , Text, Text>{
	public void reduce(Text key,Iterable<Text> values,Context context)
			throws IOException,InterruptedException{
//		for(Text v:values) {
//			double number =Math.random();
//			if(number<0.1) {
//				context.write(key,v);
//			}
//		}
//	ArrayList<Text> temp = new ArrayList<Text>();
//	for(Text v:values) {
//		temp.add(v);
//	}
//	int ramdon =(int)(Math.random()*100);
//	context.write(new Text(""),temp.get(ramdon));
//	for(int i=1;i<temp.size();i++) {
//		if((i+1)%100==0)
//			context.write(new Text(""),temp.get(i));
//	}
int ramdon =(int)(Math.random()*100);
int k=0;
		for(Text v:values) {
			if((k-ramdon)%100==0)
				context.write(new Text(""),v);
			k++;
		}
   }
}
