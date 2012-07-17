package drivers;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class WikipediaRevisionJoin {
	public static void main(String[] args) throws IOException {
		JobConf configuration = new JobConf(WikipediaRevisionJoin.class);
		configuration.setJobName("Wikipedia Data");
		
		configuration.setOutputKeyClass(IntWritable.class);
		configuration.setOutputValueClass(Text.class);
		
		configuration.setCombinerClass(Reduce.class);
		configuration.setReducerClass(Reduce.class);
		
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/training.tsv"), TextInputFormat.class, RevisionMapper.class);
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/comments.tsv"), TextInputFormat.class, CommentMapper.class);
		
		configuration.setOutputFormat(TextOutputFormat.class);
		Date today = new Date();
		FileOutputFormat.setOutputPath(configuration, new Path("/bmarcaur/data/revisions/" + today.getDay() + '_' + today.getHours() + '_' + today.getMinutes()));
		
		JobClient.runJob(configuration);
	}
	
	public static class RevisionMapper extends MapReduceBase  implements Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable revisionId = new IntWritable();
		private Text combinedColumns = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			try{
				String columns = value.toString();
				String rawRevisionId = columns.split("\\t")[2];
				revisionId.set(Integer.parseInt(rawRevisionId));
				combinedColumns.set(columns);
				output.collect(revisionId, combinedColumns);
			} catch (NumberFormatException e){}
		}
	}
	
	public static class CommentMapper extends MapReduceBase  implements Mapper<LongWritable, Text, IntWritable, Text> {
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
		
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String fullCombinedColumns = new String();
			while(values.hasNext()){
				String columns = values.next().toString();
				if(columns.split("\\t").length < 4){
					fullCombinedColumns += '\t' + columns;
				} else {
					fullCombinedColumns = columns + fullCombinedColumns;
				}
			}
			output.collect(key, new Text(fullCombinedColumns.trim()));
		}
	}
}
