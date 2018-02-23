// Name- POOJA REDDY NATHALA
//pnathala@uncc.edu
package org.myorg;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;


public class TFIDF extends Configured implements Tool {

  private static final Logger LOG = Logger .getLogger( TFIDF.class);//The class begins by initializing the logger.
   //The main method invokes ToolRunner, which creates and runs a new instance of TFIDF, passing the command line arguments.
  static long count;

   public static void main( String[] args) throws  Exception {
      //String[] tfargs={args[0],args[1]+"/tf.tmp"};
      
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
     FileSystem fs = FileSystem.get(getConf());
Path pt = new  Path(args[0]);
ContentSummary cs = fs.getContentSummary(pt);
 TFIDF.count = cs.getFileCount();// To calculate the number of files in the input folder
System.out.println(TFIDF.count);
     String scount=Long.toString(TFIDF.count);
  System.out.println(scount);

 Job job  = Job .getInstance(getConf(), " termfrequency ");// Job new instance creation with name "termfrequency"
      job.setJarByClass( this .getClass());//Set the JAR to use, based on the class in use.
     
      String outputTempDir = "/user/cloudera/wordcount/Temp";//added
      FileInputFormat.addInputPaths(job, args[0]);
     /* File f = new File(args[0]);
      if(f.isDirectory()){
      String[] f2 = f.list();
     	count = f2.length;
    	
      }*/
     
  //cast(TFIDF.count as scount);
/*FileSystem fs = FileSystem.get(getConf());
boolean recursive = false;
RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]), recursive);
while (ri.hasNext()){
    TFIDF.count+=1;
    ri.next();
}*/
      FileOutputFormat.setOutputPath(job,  new Path(outputTempDir));// This job takes the input declared in the first argument and stores the output in outTempDIr path 
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass( Text .class);// Sets the key class for the Mapper outptut data
      job.setMapOutputValueClass(IntWritable.class);// Sets the value class for the Mapper outptut data
      job.setOutputKeyClass(Text.class);// Sets the key class for the outptut data
      job.setOutputValueClass( DoubleWritable .class);// Sets the value class for the outptut data

       job.waitForCompletion(true); // this goes to next command if the job is completed successfully
   // Set the configuration with varaible count and with value stored in scount
     Configuration conf = new Configuration();
conf.set("count", scount);
   // Job 2 added
    Job job2 = Job.getInstance(conf, " TFIDF ");// Job new instance creation with name "TFIDF"
     job2.setJarByClass(this.getClass());//Set the JAR to use, based on the class in use.
     job2.setMapperClass( Map2.class);
     job2.setReducerClass(Reduce2.class);
     //job2.setInputFormatClass(KeyValueTextInputFormat.class);
// This job takes the input declared in the path outputTempDir and stores the output in secong argument
     FileInputFormat.addInputPath(job2, new Path(outputTempDir));
     FileOutputFormat.setOutputPath(job2, new Path(args[1]));
     job2.setMapOutputKeyClass(Text.class);// Sets the key class for the Mapper outptut data
     job2.setMapOutputValueClass(Text.class);// Sets the value class for the Mapper outptut data
     
     job2.setOutputKeyClass( Text .class);//Sets the key class for the outptut data
     job2.setOutputValueClass(DoubleWritable .class);//Sets the value class for the outptut data
   
     return job2.waitForCompletion( true)  ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1

       
    

   }
  
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable(1);// global varaibles used in this class
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");// a regular expression pattern you can use to parse each line of input text on word boundaries ("\b").

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException { //Hadoop invokes the map method once for every key/value pair from your input source.
      //  Path filePath=((FileSpilt) context.getInputSplit()).getPath();
         FileSplit filesplit=(FileSplit)context.getInputSplit();
        String filename= filesplit.getPath().getName();// to get the filename which has the corresponding line
         String line  = lineText.toString();
       Text currentWord  = new Text();
           
         for ( String word  : WORD_BOUNDARY .split(line)) {// regualr expression pattern is used to split the input line string
            if (word.isEmpty()) {
               continue;
            }
           //  String delimiter="##";
            word= word.toLowerCase();
            String cword= word+"#####"+filename+"\t";// defining key as the combination of word and filename
            currentWord  = new Text(cword);
            context.write(currentWord,one);//  writes a intermediate key/value pair to the context object for the job.
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override
      public void reduce( Text cword,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {// The reducer processes each pair, adding one to the count for the current word in the key/value pair to the overall count of that word from all mappers
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         double TF=( 1+ Math.log10(sum));// Termfrequency calculation step
        context.write(cword,  new DoubleWritable(TF));// writes the result for that word to the reducer context object, and moves on to the next
      }
   }

     // Mapper2 added
      public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > { 
     
      private Text word  = new Text();
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {//Hadoop invokes the map method once for every key/value pair from your input source.
         String line  = lineText.toString();
     // following four steps converts the <kay,value> ihe form <"yellow#####file1.txt",1> to the form <"yellow", "file1.txt=1.0">
    String s1 = line.substring(0, line.indexOf('#'));
    String s2 = line.substring(line.lastIndexOf('#')+1, line.length());
    String a[] = s2.split("\t\t");
    String value = a[0]+"="+a[1];

            context.write(new Text(s1),new Text(value));// writes the result as intermediate key/ value pairs
         }
      }
      
      public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
          @Override
          public void reduce( Text cword,  Iterable<Text > counts,  Context context)
             throws IOException,  InterruptedException {// The reducer processes each pair, and finds the number of values(x) present for each key and calculates the IDF values 
                Configuration conf = context.getConfiguration();
                String param = conf.get("count");// gets the values stored in conf varaible count
                long paramCount=Long.parseLong(param);

        	  
        	  int x =0;
               
               
               ArrayList<String> alist = new ArrayList<String>();
              
               for(Text count: counts ){
            	   x++;
            	   alist.add(count.toString());
            	   
               
               }
               
        	  
        	  for(String count: alist ){
        		
        	  
             double TF=Math.log10(( 1+ paramCount/x));


        	 String s = count;
        	 
        	 String a[] = s.split("=");
		
        	 double db =Double.parseDouble(a[1]);

		       	 
             TF = TF*db;// Step which calculates the TFIDF values for each word 

		
             
             String k = (cword.toString()) + "#####"+ a[0];//defines key in the form <"word#####file1.txt">
             
            context.write(new Text(k),  new DoubleWritable(TF));// writes the result for that word to the reducer context object, and moves on to the next
            
        	  }
          }
       }
      
      
   }

