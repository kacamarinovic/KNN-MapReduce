package knnpackage;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Reducer;
public class KNNReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		
		int k = context.getConfiguration().getInt("k_value", 5);
		ArrayList<String> dist=new ArrayList<String>();		
		for(Text value:values)
		{
			dist.add(value.toString());
		}		
		//izdvajanje k najblizih suseda
		Collections.sort(dist);
		String[] labels=new String[k];
		String temp;
		for(int j=0;j<k;j++)
		{
			temp=dist.get(j);
			labels[j]=String.valueOf(temp.replaceAll("[\\d.]",""));
			System.out.println(labels[j]);
			
		}	
		//odredjivanje koja je klasa najzastupljenija medju k suseda		
		HashMap<String,Integer> map=new HashMap<String,Integer>();		
		String str="";
		int maxvalue=-1;		
		for(int i=0;i<k;i++) {
			if(!map.containsKey(labels[i]))
			{
				map.put(labels[i],1);				
			}
			else
			{
				map.put(labels[i],map.get(labels[i])+1);				
			}
		}
		System.out.println(map.size());
		Iterator<Entry<String, Integer>> it=map.entrySet().iterator();
		str=((Entry<String, Integer>)it.next()).getKey().toString();		
		while(it.hasNext())
		{
			Entry<String, Integer> entry=(Entry<String, Integer>)it.next();
			if(Integer.parseInt(entry.getValue().toString())>maxvalue)
			{
				str=entry.getKey().toString();
				maxvalue=Integer.parseInt(entry.getValue().toString());
			}
		}		
		context.write(key,new Text("Class: "+str));
		
	}
	
	

}
