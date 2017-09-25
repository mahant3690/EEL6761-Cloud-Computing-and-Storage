import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheWordCount extends Configured implements Tool {

public static class WordCountMapper
		extends Mapper<Object, Text, Text, IntWritable>
{
	List<String> searchList = new ArrayList<String>();
	private Text word = new Text();
	private final static IntWritable one = new IntWritable( 1 );
	
	protected void setup( Mapper<Object, Text, Text, IntWritable>.Context context )
							throws IOException, InterruptedException
	{
		URI[] searchListFiles = context.getCacheFiles();
		BufferedReader reader = new BufferedReader( new FileReader(searchListFiles[0].toString()) );
		while ( true )
		{
			String line = reader.readLine();
			if ( line == null )
				break;
			
			StringTokenizer tokenizer = new StringTokenizer( line );	
			while ( tokenizer.hasMoreTokens() )
				searchList.add( tokenizer.nextToken() );			
		}

		reader.close();
		super.setup( context );
	}
	
	public void map( Object key, Text value, Context context
						) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString());	
		while ( itr.hasMoreTokens() )
		{
			final String current = itr.nextToken();
			if ( searchList.contains(current) )
			{
				word.set( current );
				context.write( word, one );
			}
		}
	}
}
 
public static class WordCountReducer
		extends Reducer<Text,IntWritable,Text,IntWritable>
{
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,
						Context context
                 		) throws IOException, InterruptedException
	{
		int sum = 0;
		for ( IntWritable val : values )
			sum += val.get();

		result.set( sum );
		context.write( key, result );
	}
}
 
public int run(String[] args) throws Exception
{
	Job job = Job.getInstance(new Configuration(), "word count");
	List<String> paths = new ArrayList<String>();

	for ( int idx = 0; idx<args.length; idx++ )
	{
		if ( "-cacheFile".equals( args[idx] ) )
			job.addCacheFile( new URI(args[++idx]) );	
		else
			paths.add( args[idx] );
	}
	
	job.setJarByClass(WordCount.class);  
	job.setMapperClass( WordCountMapper.class );  
	job.setCombinerClass(WordCountReducer.class);
	job.setReducerClass(WordCountReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(paths.get(0)));
	FileOutputFormat.setOutputPath(job, new Path(paths.get(1)));
	return job.waitForCompletion(true) ? 0 : 1;
}

/** Usage: Command line: hadoop jar jarfile.jar DistributedCacheWordCount /input output --cacheFile /inputdataforcachefile
 */

public static void main( String[] args ) throws Exception
{
	final int res = ToolRunner.run( new Configuration(), new DistributedCacheWordCount(), args );
	System.exit( res );
}
}