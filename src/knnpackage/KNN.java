package knnpackage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class KNN {

	public static void main(String[] args) throws Exception {
		System.out.println("KNN!\n");
		Configuration conf=new Configuration();
		
		if(args.length<5) {
			System.err.println("Usage: KNN <testDatasetPath> <trainDatasetPath> <outputPath> <k> <n>");
			System.exit(2);
		}
		
		Path cartInputPath = cartesian(args[0], args[1], conf);

		
		Job job = Job.getInstance(conf, "KNN Classification");
		job.setJarByClass(KNN.class);
	
		
		FileInputFormat.setInputPaths(job,cartInputPath); 
		FileOutputFormat.setOutputPath(job,new Path(args[2])); 
		conf.setInt("k_value", Integer.parseInt(args[3]));
		conf.setInt("numoffeatures",Integer.parseInt(args[4]));
		job.setMapperClass(KNNMapper.class);
		job.setReducerClass(KNNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
	
	public static Path cartesian(String testDatasetPath, String trainDatasetPath, Configuration conf) throws Exception  {
		
		FileSystem fs = FileSystem.get(conf);
		
		Path cartPath = new Path("/cart");
		if(fs.exists(cartPath)) {
			fs.delete(cartPath,true);
		}
		fs.mkdirs(cartPath);
		
		BufferedReader testReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testDatasetPath))));
		String testLine;
		
		FSDataOutputStream outputStream = fs.create(new Path(cartPath, "cart_data.txt"), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
		
		while((testLine = testReader.readLine()) != null) {
			BufferedReader trainReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainDatasetPath))));
			
			String trainLine;
	        while ((trainLine = trainReader.readLine()) != null) {
	        	
	        	String cartLine = testLine + "&" + trainLine;
	        	writer.write(cartLine);
	        	writer.newLine();
	        	
	        }
	        
	        trainReader.close();
		}
		writer.close();
		
		testReader.close();
		
		return cartPath;

	}
}
