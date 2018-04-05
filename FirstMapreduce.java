package marketVolality;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FirstMapreduce {

	public static class firstMapper extends Mapper<Object, Text, Text, Text> {
		private Text firstKey = new Text();
		private Text firstValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineparts = line.split(",");
			if (!lineparts[0].equals("Date")) {
				String[] yearMonthDay = lineparts[0].split("-");

				String firstkey = getFileName(context) + "-" + yearMonthDay[0] + "-" + yearMonthDay[1];
				firstKey.set(firstkey);
				String DayAndClosingPrising = yearMonthDay[2] + "," + lineparts[4];

				firstValue.set(DayAndClosingPrising);
				context.write(firstKey, firstValue);
			}

		}

		public String getFileName(Context context) {
			return ((FileSplit) context.getInputSplit()).getPath().getName();
		}
	}

	public static class firstReducer extends Reducer<Text, Text, Text, Text> {
		private Text secondValue = new Text();

		public void reduce(Text firstKey, Iterable<Text> firstValue, Context context)
				throws IOException, InterruptedException {
			HashMap<Integer, Double> dailyClosingPrice = new HashMap<>();
			HashDailyClosingPrice(dailyClosingPrice, firstValue);
			String stringOutput = " ";
			double sumOfDailyReturn = 0.00;
			int numOfDay = dailyClosingPrice.size();
			DecimalFormat df = new DecimalFormat("0.000");
			for (HashMap.Entry<Integer, Double> entry : dailyClosingPrice.entrySet()) {
				int today = entry.getKey();
				double todayClosingPrice = entry.getValue();
				int theLastDay = searchLastDay(dailyClosingPrice, today);
				double lastClosingPrice;
				if (theLastDay != 31) {
					lastClosingPrice = dailyClosingPrice.get(theLastDay);
					double dailyReturn = todayClosingPrice - lastClosingPrice;
					df.format(dailyReturn);
					stringOutput += today + ":" + dailyReturn + ",";
					sumOfDailyReturn += dailyReturn;
				}
			}
			double meanOfDailyReturn = sumOfDailyReturn / numOfDay;
			stringOutput += "mean:" + meanOfDailyReturn;
			secondValue.set(stringOutput);
			context.write(firstKey, secondValue);
		}

		public void HashDailyClosingPrice(HashMap<Integer, Double> dailyClosingPrice, Iterable<Text> firstValue) {
			for (Text line : firstValue) {
				String stringline = line.toString();
				String[] linecomponent = stringline.split(",");
				double ClosingPrice = Double.parseDouble(linecomponent[1]);
				dailyClosingPrice.put(Integer.parseInt(linecomponent[0]), ClosingPrice);
			}
		}

		public int searchLastDay(HashMap<Integer, Double> dailyClosingPrice, int today) {
			int NumDaysBeforeToday = 1;
			while (NumDaysBeforeToday < 31) {
				if (dailyClosingPrice.containsKey(today - NumDaysBeforeToday))
					return today - NumDaysBeforeToday;
				else
					++NumDaysBeforeToday;
			}
			return 31;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJar("FirstMapreduce.jar");
		job.setMapperClass(firstMapper.class);
		job.setReducerClass(firstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
