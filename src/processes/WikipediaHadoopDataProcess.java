package processes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

@SuppressWarnings("deprecation")
public class WikipediaHadoopDataProcess {
	public static void main(String[] args) throws IOException {
		JobConf configuration = new JobConf(WikipediaHadoopDataProcess.class);
		configuration.setJobName("Wikipedia Data");
		
		configuration.setOutputKeyClass(IntWritable.class);
		configuration.setOutputValueClass(Text.class);
		
		configuration.setCombinerClass(Reduce.class);
		configuration.setReducerClass(Reduce.class);
		
		MultipleInputs.addInputPath(configuration, new Path("/usr/localadmin/wikidata/training.tsv"), TextInputFormat.class, RevisionMapper.class);
		MultipleInputs.addInputPath(configuration, new Path("/usr/localadmin/wikidata/comments.tsv"), TextInputFormat.class, CommentMapper.class);
		
		configuration.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(configuration, new Path("/usr/localadmin/wikidata/output/revisions/"));
		
		JobClient.runJob(configuration);
	}
	
	public static class RevisionMapper extends MapReduceBase  implements Mapper<IntWritable, Text, IntWritable, Text> {
		private IntWritable revisionId = new IntWritable();
		private Text combinedColumns = new Text();
		
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String columns = value.toString();
			String rawRevisionId = columns.split("\\t")[2];
			revisionId.set(Integer.parseInt(rawRevisionId));
			combinedColumns.set(columns);
			output.collect(revisionId, combinedColumns);
		}
	}
	
	public static class CommentMapper extends MapReduceBase  implements Mapper<IntWritable, Text, IntWritable, Text> {
		private IntWritable revisionId = new IntWritable();
		private Text combinedColumns = new Text();
		
		public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String columns = value.toString();
			String rawRevisionId = columns.split("\\t")[0];
			revisionId.set(Integer.parseInt(rawRevisionId));
			combinedColumns.set(columns);
			output.collect(revisionId, combinedColumns);
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
					fullCombinedColumns = columns + '\t' + fullCombinedColumns;
				}
			}
			output.collect(key, new Text(fullCombinedColumns.trim()));
		}
	}
}
