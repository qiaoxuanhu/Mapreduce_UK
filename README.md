# Mapreduce_UK
//WordCount.java
import java.awt.List;
import java.io.IOException;

import java.util.ArrayList; //I am not sure i need import it or not
import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	
	public static String INPUT_PATH="/Users/kaya/Desktop/big data analytics/Industrial strategy";
	public static String OUTPUT_PATH="/Users/kaya/Desktop/big data analytics/Output";
	public static String OUTPUT_FILENAME="Output.txt";

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String regex = "[a-zA-Z]{2,}";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	List banList = new ArrayList();
    	banList.add("the");
    	banList.add("and");
    	banList.add("of");
    	banList.add("or");
    	banList.add("off");
    	banList.add("an");
    	banList.add("on");
    	
    List<String> cleanedTokens = CleanLine(value.toString());
    for (int i = 0; i < cleanedTokens.size();i = i + 1){
    	String currentCleanedToken = cleanedTokens.get(i).toLowerCase();
    	if(!banList.contains(currentCleanedToken)) {
    		word.set(currentCleanedToken);
    		context.write(word,one);
    		
    	}
    }
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {

        word.set(itr.nextToken());

        context.write(word, one);

      }

    }

  }



  public static class IntSumReducer

       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();



    public void reduce(Text key, Iterable<IntWritable> values,

                       Context context

                       ) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {

        sum += val.get();

      }

      result.set(sum);

      context.write(key, result);

    }

  }



  public static void main(String[] args) throws Exception {

    //BasicConfigurator.configure();

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);

    job.setMapperClass(TokenizerMapper.class);

    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}

*/


// MyOutPutFormat.java
import java.io.File;
import java.io.FileWriter;   //not sure
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class MyOutPutFormat extends OutputFormat<Text,IntWritable> {

	//private FileWriter outputWritter = null;
	
	@Override
	public Recordwriter<Text,IntWritable> getRecordWriter(
		TaskAttemptContext context) throws IOException,
		InterruptedException {
		File file = new File(WordCount.OUTPUT_PATH + "\\" + WordCount.OUTPUT_FILENAME);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}
		this.outputWritter = new FileWriter(file);
		return new MyRecordWriter(outputWritter);
	}
	
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(new Path(WordCount.OUTPUT_PATH)) //HOW CAN I WRITE A PATH?
	}
	
}

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}


//MyRecordWriter.java
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class MyRecordWriter extends RecordWriter<Text,IntWritable> {

	private FileWriter outputWritter = null;
	
	public MyRecordWriter(FileWriter outputStream){
		this.outputWritter = outputStream;
	}
//the color of key and value
    public void write(Text key, IntWritable value) throws IOException,
        InterruptedException{
      this.outputWritter.write(key.toString());
      this.outputWritter.write(" ");
      this.outputWritter.write(String.valueOf(value.get())); //what do I need to write about value?
      this.outputWritter.write("\r\n");//might be wrong about return
    }
    
    public void close(TaskAttemptContext context) throws IOException
        InterruptedException {
      this.outputWritter.close();
    }
}
    }
  
  
//Regexhelper.java
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Regexhelper {
    public static List<String> CleanString(String contentText, String regexText){  //color of string
    	Pattern pattern = Pattern.compile(regexText);
    	Matcher matcher = pattern.matcher(contentText);
    	List<String> list = new ArrayList<>();
    	while (matcher.find()) {   //color of find,add,group
    		list.add(matcher.group());
    	}
    	return list;
    }
}

//InnerCalculator.java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InnerCalculator {
     public int InnerNumber1;
     public int InnverNumber2;
}


//my coding
import java.io.IOException;

import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	
	public static String INPUT_PATH="";
	public static String OUTPUT_PATH="";
	public static String OUTPUT_FILENAME="";
	
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String regex = "[a-zA-Z]{2,}";



  public static class TokenizerMapper

       extends Mapper<Object, Text, Text, IntWritable>{



    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();



    public void map(Object key, Text value, Context context

                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {

        word.set(itr.nextToken());

        context.write(word, one);

      }

    }

  }



  public static class IntSumReducer

       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();



    public void reduce(Text key, Iterable<IntWritable> values,

                       Context context

                       ) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {

        sum += val.get();

      }

      result.set(sum);

      context.write(key, result);

    }

  }



  public static void main(String[] args) throws Exception {

    //BasicConfigurator.configure();

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(WordCount.class);

    job.setMapperClass(TokenizerMapper.class);

    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}

	
    Configuration conf = new Configuration();
    Configuration conf = new Configuration();
