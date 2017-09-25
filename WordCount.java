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

  public static class SingleWordCountMapper
       extends Mapper<Object, Text, Text, IntWritable>
  {
    private final static IntWritable one = new IntWritable( 1 );
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException
    {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while ( itr.hasMoreTokens() )
      {
        word.set( itr.nextToken() );
        context.write( word, one );
      }
    }
  }
  
  public static class DoubleWordCountMapper
  		extends Mapper<Object, Text, Text, IntWritable>
  {
	 private final static IntWritable one = new IntWritable( 1 );
	 private Text word = new Text();

	 public void map(Object key, Text value, Context context
			 		) throws IOException, InterruptedException
	 {
		 StringTokenizer itr = new StringTokenizer(value.toString());
		 while ( itr.hasMoreTokens() )
		 {
			 StringBuffer sb = new StringBuffer();
			 sb.append( itr.nextToken() );
			 if ( !itr.hasMoreTokens() )
				 break;
			 
			 sb.append( " " );
			 sb.append( itr.nextToken() );
			 word.set( sb.toString() );
			 context.write( word, one );
		 }
	 }
  }

  public static class WordCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for ( IntWritable val : values )
        sum += val.get();
      
      result.set( sum );
      context.write( key, result );
    }
  }
  
  /** Usage: Command line: hadoop jar jarfile.jar WordCount /input output WordCountType 
   * 
   * WordCountType would be Single if no. of single words need to be computed,
   * else it would be Double. This is necessary, else no results will be computed.
   */
  
  public static void main( String[] args ) throws Exception
  {    
    Job job = Job.getInstance(new Configuration(), "word count");
    job.setJarByClass(WordCount.class);
    if ( args[2] == "Single" )
    	job.setMapperClass( SingleWordCountMapper.class );
    else if ( args[2] == "Double" )
    	job.setMapperClass( DoubleWordCountMapper.class );
    
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}