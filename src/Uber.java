package com.yangyang.hadoop;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
//import java.util.StringTokenizer;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public static class Uber {
public static class uberMapper extends Mapper<Object, Text, Text, IntWritable>{
	 private int trip;
	 private String[] week= {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};
	 private Text nextKey=new Text();
	 private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	 
	 private int weekDay; 
public void  map(Object key,Text value, Context context) throws IOException, InterruptedException {   
		 String line =value.toString();
		 String[] token=line.split(",");
		 Date sequenceDate= new Date();
		 try {
			 
			 sequenceDate=sdf.parse(token[1]);
			  
			 } catch (ParseException e) {
			  
			 e.printStackTrace();
			  
			 }
		
		 Calendar cal = Calendar.getInstance();
		 cal.setTime(sequenceDate);
		 weekDay = cal.get(Calendar.DAY_OF_WEEK);
		 nextKey.set (new Text(token[0]+week[weekDay]));
		 trip=Integer.parseInt(token[2]);
		 context.write(nextKey, new IntWritable(trip));
		 }

	}

public static class uberReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	  private IntWritable iwSum=new IntWritable();
	public void reduce(Text nextkey, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable val:values) {
			sum+=val.get();
		}
		iwSum.set(sum);
		context.write(nextkey, iwSum);
	}
}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length < 2) {
        System.err.println("Usage: wordcount <in> [<in>...] <out>");
        System.exit(2);
      }
	Job job = Job.getInstance(conf,"uber");
	job.setJar("Uber.class");
	job.setMapperClass(uberMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(uberReducer.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	 }
	 FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}

