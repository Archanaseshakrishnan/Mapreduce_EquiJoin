

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	public static String t1="";
	public static String t2="";
public static class MyMapper extends Mapper<Object, Text, Text, Text>{
	private String thiskey;
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String[] words=value.toString().split(",");
		thiskey = words[1];
		if(!(t1.equals(""))){
			if(!(t1.equals(words[0]))){
				t2=words[0];
			}
		}
		else{
			t1=words[0];
		}
		context.write(new Text(thiskey), value);
	}
}

private static class MyReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		//ArrayList<String> t1=new ArrayList<String>();
		//ArrayList<String> t2=new ArrayList<String>();
		String result1="";
		IntWritable count=new IntWritable(0);
		for(Text val:values){
			String[] temp=val.toString().split(",");
			for(String t:temp){
				if(t.equals(t1)){
					int tem=count.get();
					tem++;
					count=new IntWritable(tem);
				}
				if(t.equals(t2)){
					int tem=count.get();
					tem++;
					count=new IntWritable(tem);
				}
			}
			String result = val.toString();
			result1=result+", "+result1;
		}
			//context.write(new Text(count.toString()),NullWritable.get());
			if(count.toString().equals("2")){
			
			result1=result1.replaceAll(", $","");
			
			context.write(new Text(result1),NullWritable.get());
			
		}
		
		//context.write(vis, eval);
	}
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "equi join");
    job.setJarByClass(equijoin.class);
    job.setMapperClass(MyMapper.class);
    //job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
