/**
 * 
 */
package marketVolality;

import java.util.regex.Pattern;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author yangyangwang
 *
 */
public class secondMapreduce {
	public static class secondMapper extends Mapper<Text, Text, Text, Text> {
		private Text secondkey = new Text();
		private Text secondvalue = new Text();

		public void map(Text firstkey, Text firstvalue, Context context) throws IOException, InterruptedException {
			String dailyReturnInEveryMonth = firstvalue.toString();
			String[] dayAndDailyReturns = dailyReturnInEveryMonth.split(",");
			Pattern p = Pattern.compile("mean");
			double meanDailyReturn = 0;
			for (String dayAndDailyReturn : dayAndDailyReturns) {
				Matcher m = p.matcher(dayAndDailyReturn);
				if (m.find()) {
					String[] dayOrDailyReturn = dayAndDailyReturn.split(":");
					meanDailyReturn = Double.parseDouble(dayOrDailyReturn[1]);
				}
			}
			for (String DayAndDailyReturn : dayAndDailyReturns) {

				String[] DayOrDailyReturn = DayAndDailyReturn.split(":");
				String key = firstkey.toString();
				secondkey.set(key);
				String value = DayOrDailyReturn[1] + "," + Double.toString(meanDailyReturn);
				secondvalue.set(value);
				context.write(secondkey, secondvalue);

			}

		}

}		
		
		public static class secondReducer extends Reducer<Text,Text,Text,DoubleWritable>{
			private DoubleWritable volality=new DoubleWritable();
			public void reduce(Text key,Iterable<Text> value,Context context)throws IOException, InterruptedException  {
				
				int daysNumsInOneMonth=0;
				double sumOfDeviation=0;
				for(Text dailyReturnAndMean:value) {
					
					String [] dailyOrReturn=dailyReturnAndMean.toString().split(",");
					
					double dailyReturn=Double.parseDouble(dailyOrReturn[0]);
					double meanDailyReturn=Double.parseDouble(dailyOrReturn[1]);
					double deviation=Math.pow((dailyReturn-meanDailyReturn),2);
					sumOfDeviation+=deviation;
					daysNumsInOneMonth+=1;
					}
				    double variance=sumOfDeviation/(daysNumsInOneMonth-1);
				    volality.set(variance);
				    context.write(key,volality);
				    }
			}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJar("secondMapreduce.jar");
		job.setMapperClass(secondMapper.class);
		job.setReducerClass(secondReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}	
}
		// TODO Auto-generated method st
