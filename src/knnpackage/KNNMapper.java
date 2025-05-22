package knnpackage;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class KNNMapper extends Mapper<LongWritable, Text, Text, Text>{

	public static int numoffeatures;
	String testStr="";
	
	public static float euclideandist(Float[] test,Float[] train,int n)
	{
		float distance=0;
		for(int j=0;j<n;j++)
		{
			distance+=Math.pow((test[j]-train[j]),2);
		}
		distance=(float)Math.sqrt(distance);
		return distance;
	}
	public void setup(Context context) throws IOException, InterruptedException
	{
		
		numoffeatures=context.getConfiguration().getInt("numoffeatures",4);		
		
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] input=value.toString().split("&");
		testStr = input[0];
		String trainStr = input[1];
		
		String[] testPoint = testStr.split("\\ ");
		String[] trainPoint = trainStr.split("\\ ");
				
		Float[] testF = new Float[numoffeatures];
		Float[] trainF = new Float[numoffeatures];
		for(int i=0;i<numoffeatures;i++)
		{
			testF[i]=Float.parseFloat(testPoint[i]);
			trainF[i]=Float.parseFloat(trainPoint[i]);
		}
		
		String classLab = trainPoint[numoffeatures]; 
		String vrednost = String.valueOf(euclideandist(testF,trainF,numoffeatures))+classLab;
		context.write(new Text(testStr), new Text(vrednost));
		
	}
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		
	}
}
