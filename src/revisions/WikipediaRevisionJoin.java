package revisions;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import articles.mappers.RevisionMapper;

import revisions.mappers.CommentMapper;
import revisions.reducers.CommentReducer;

public class WikipediaRevisionJoin {
	public static void main(String[] args) throws IOException {
		// Configure the job
		JobConf configuration = new JobConf(WikipediaRevisionJoin.class);
		configuration.setJobName("Wikipedia Data");
		configuration.setOutputKeyClass(IntWritable.class);
		configuration.setOutputValueClass(Text.class);
		configuration.setCombinerClass(CommentReducer.class);
		configuration.setReducerClass(CommentReducer.class);
		configuration.setOutputFormat(TextOutputFormat.class);
		
		// Bind specific maps to particular inputs
		// Used to map in parallel, but reduce collectively
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/training.tsv"), TextInputFormat.class, RevisionMapper.class);
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/comments.tsv"), TextInputFormat.class, CommentMapper.class);
		
		// Run the job
		Calendar cal = Calendar.getInstance();
		FileOutputFormat.setOutputPath(configuration, new Path("/bmarcaur/data/revsions/" + cal.get(Calendar.DATE) + '_' + cal.get(Calendar.HOUR) + '_' + cal.get(Calendar.MINUTE)));
		JobClient.runJob(configuration);
	}
}
