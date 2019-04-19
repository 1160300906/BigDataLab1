package bigdataLab1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessedReducer extends Reducer<Text, Text , Text, Text>{
	public void reduce(Text key,Iterable<Text> values,Context context)
			throws IOException,InterruptedException{
		double sumrate=0;
		double sumincome=0;
		int n1=0;
		int n2=0;
		ArrayList<String> ratelist=new ArrayList<String>(); 
		ArrayList<String> incomelist=new ArrayList<String>();
		ArrayList<String> list=new ArrayList<String>();
		for(Text v:values) {
			String[] ttt=v.toString().split("\\|");
			if(!ttt[6].equals("?")) {
				n1++;
				sumrate=sumrate+Double.valueOf(ttt[6]);
			if(!ttt[11].equals("?")) {
				n2++;
				sumincome=sumincome+Double.valueOf(ttt[11]);
				context.write(new Text(""),v);
			}else
			incomelist.add(v.toString());
			}else {				
				if(!ttt[11].equals("?")) {
					n2++;
					sumincome=sumincome+Double.valueOf(ttt[11]);	
					ratelist.add(v.toString());		
				}else
					list.add(v.toString());
			}			
		}
		for(int i=0;i<ratelist.size();i++) {
			String[] ttt=ratelist.get(i).split("\\|");
			String s=ttt[0]+"|"+ttt[1]+"|"+ttt[2]+"|"+ttt[3]+"|"+ttt[4]+"|"+
					ttt[5]+"|"+String.format("%.2f",sumrate/n1)+"|"+ttt[7]+"|"+ttt[8]+"|"+ttt[9]+"|"+
					ttt[10]+"|"+ttt[11];
			context.write(new Text(""),new Text(s));
		}
		for(int i=0;i<list.size();i++) {
			String[] ttt=list.get(i).split("\\|");
			String s=ttt[0]+"|"+ttt[1]+"|"+ttt[2]+"|"+ttt[3]+"|"+ttt[4]+"|"+
					ttt[5]+"|"+String.format("%.2f",sumrate/n1)+"|"+ttt[7]+"|"+ttt[8]+"|"+ttt[9]+"|"+
					ttt[10]+"|"+String.format("%.0f",sumincome/n2);
			context.write(new Text(""),new Text(s));
		}
		for(int i=0;i<incomelist.size();i++) {
			String[] ttt=incomelist.get(i).split("\\|");
			String s=ttt[0]+"|"+ttt[1]+"|"+ttt[2]+"|"+ttt[3]+"|"+ttt[4]+"|"+
					ttt[5]+"|"+ttt[6]+"|"+ttt[7]+"|"+ttt[8]+"|"+ttt[9]+"|"+
					ttt[10]+"|"+String.format("%.0f",sumincome/n2);
			context.write(new Text(""),new Text(s));
		}
	}
}

