package revisions.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CommentMapper extends MapReduceBase  implements Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable revisionId = new IntWritable();
	private Text combinedColumns = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		try{
			String columns = value.toString();
			String rawRevisionId = columns.split("\\t")[0];
			revisionId.set(Integer.parseInt(rawRevisionId));
			combinedColumns.set(columns.split("\\t")[1]);
			output.collect(revisionId, combinedColumns);
		} catch (NumberFormatException e){}
	}
}
