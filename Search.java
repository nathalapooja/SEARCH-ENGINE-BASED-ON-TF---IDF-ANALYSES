// Name- POOJA REDDY NATHALA
//pnathala@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;




public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);//The class begins by initializing the logger.
   //The main method invokes ToolRunner, which creates and runs a new instance of Search, passing the command line arguments.
   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
       /*System.out.println("Please Enter Search String:");// scanner to store the query entered
      Scanner sc = new Scanner(System.in);
      inpu = sc.nextLine();
      sc.close();
      Configuration conf = new Configuration();
      conf.set("input",inpu);// Set the value of the name "input" as inpu*/
      Configuration conf=new Configuration();
      String query="";
	for(int i=2;i<args.length;i++){   // query is stored as args[2] and args[3]
		query=(query+args[i]+" ");    
                 query=query.toLowerCase();
		conf.setStrings("QueryInput",query);  //Set the value of the name "QueryInput" as query
			
	}
      Job job  = Job .getInstance(conf, " Search ");// Job new instance creation with name "Search"
      job.setJarByClass( this .getClass());//Set the JAR to use, based on the class in use.
      // This job takes the input declared in the first argument and stores the output in second argument
      
       System.out.println(args[0]);
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass(Text.class);// Sets the key class for the Mapper outptut data
      job.setMapOutputValueClass(DoubleWritable.class);// Sets the value class for the Mapper outptut data
      job.setOutputKeyClass( Text .class);// Sets the key class for the outptut data
      job.setOutputValueClass( DoubleWritable .class);// Sets the value class for the outptut data

      return job.waitForCompletion( true)  ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private Text word  = new Text();// global varaibles used in this class

      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException { //Hadoop invokes the map method once for every key/value pair from your input source.

         String line  = lineText.toString();//The input line string is derived from the output of TFIDF. java  
         
         
          Configuration conf = context.getConfiguration();
                String param = conf.get("QueryInput");// get the value of the name "QueryInput" and store it in param string
          String wordsEn[] = param.split(" ");// input query string splitted by " " are stored into string array
         for(String s1 : wordsEn){// for each text line string, if the line contains query word then that file name and particular TFIDF value is written as intermediate key/value pairs
        	 if(line.contains(s1)){
        		 int li = line.lastIndexOf("#");
        		 String spl[] = line.split("\t");
        		 String txt = spl[0].substring(li+1, spl[0].length());
        		 Double dvalue = Double.parseDouble(spl[1]);
                          String str = line.substring(0,line.indexOf("#"));
                         if(str.equals(s1))
        		 context.write(new Text(txt), new DoubleWritable(dvalue));
        		 
        	 }
        	 
        	 
        	 
         }
       
         
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
        
         double sum  = 0;
         for ( DoubleWritable count  : counts) {// Reducer operates on the combined result generated from different mappers and calculates the total sum of TFIDF values for the query string 
            sum  += count.get();
         }
         context.write(word,  new DoubleWritable(sum));// writes the result<"filename","Sum of TFIDF values"> for that query string  to the reducer context object, and moves on to the next
      }
   }
   
}

