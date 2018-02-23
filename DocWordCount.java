// Name- POOJA REDDY NATHALA
//pnathala@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);//The class begins by initializing the logger.
   //The main method invokes ToolRunner, which creates and runs a new instance of DocWordCount, passing the command line arguments. 
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");// Job new instance creation with name "wordcount"
      job.setJarByClass( this .getClass());//Set the JAR to use, based on the class in use.
      // This job takes the input declared in the first argument and stores the output in second argument
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);// Sets the key class for the outptut data
      job.setOutputValueClass( IntWritable .class);//Sets the value class for the outptut data

      return job.waitForCompletion( true)  ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);// global varaibles used in this class
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");// a regular expression pattern you can use to parse each line of input text on word boundaries ("\b"). 

      public void map( LongWritable offset,  Text lineText,  Context context) //Hadoop invokes the map method once for every key/value pair from your input source.
        throws  IOException,  InterruptedException {
      //  Path filePath=((FileSpilt) context.getInputSplit()).getPath();
         FileSplit filesplit=(FileSplit)context.getInputSplit();
        String filename= filesplit.getPath().getName();// to get the filename which has the corresponding line
         String line  = lineText.toString();
       Text currentWord  = new Text();
            
         for ( String word  : WORD_BOUNDARY .split(line)) {// regular expression pattern is used to split the input line string
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

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text cword,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {// The reducer processes each pair, adding one to the count for the current word in the key/value pair to the overall count of that word from all mappers
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         context.write(cword,  new IntWritable(sum));// writes the result for that word to the reducer context object, and moves on to the next
      }
   }
}
