package marketVolality;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;   
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class Driver extends Configured implements Tool {

 public int run(String[] args) throws Exception {

    JobControl jobControl = new JobControl("jobChain"); 
    Configuration conf1 = getConf();

    Job job1 = Job.getInstance(conf1);  
    job1.setJar("FirstMapreduce.jar");
    job1.setJobName("first");

    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

    job1.setMapperClass(FirstMapreduce.firstMapper.class);
	job1.setReducerClass(FirstMapreduce.firstReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);

    ControlledJob controlledJob1 = new ControlledJob(conf1);
    controlledJob1.setJob(job1);

    jobControl.addJob(controlledJob1);
    Configuration conf2 = getConf();

    Job job2 = Job.getInstance(conf2);
    job2.setJar("secondMapreduce.jar");
    job2.setJobName("second");

    FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

    job2.setMapperClass(secondMapreduce.secondMapper.class);
	job2.setReducerClass(secondMapreduce.secondReducer.class);
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(DoubleWritable.class);
    ControlledJob controlledJob2 = new ControlledJob(conf2);
    controlledJob2.setJob(job2);

    // make job2 dependent on job1
    controlledJob2.addDependingJob(controlledJob1); 
    // add the job to the job control
    jobControl.addJob(controlledJob2);
    Thread jobControlThread = new Thread(jobControl);
    jobControlThread.start();

while (!jobControl.allFinished()) {
    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
try {
    Thread.sleep(5000);
    } catch (Exception e) {

    }

  } 
   System.exit(0);  
   return (job1.waitForCompletion(true) ? 0 : 1);   
  } 
  public static void main(String[] args) throws Exception { 
  int exitCode = ToolRunner.run(new Driver(), args);  
  System.exit(exitCode);
  }
}