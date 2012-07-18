package articles;

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

import articles.mappers.ArticleMapper;
import articles.mappers.RevisionMapper;
import articles.reducers.ArticleReducer;



public class WikipediaArticleJoin {
	public static void main(String[] args) throws IOException {
		// Set up the job configuration
		JobConf configuration = new JobConf(WikipediaArticleJoin.class);
		configuration.setJobName("Wikipedia Articles");
		configuration.setOutputKeyClass(IntWritable.class);
		configuration.setOutputValueClass(Text.class);
		configuration.setOutputFormat(TextOutputFormat.class);
		// Tell the configuration to use my reducer
		configuration.setReducerClass(ArticleReducer.class);
		
		// Bind specific maps to particular inputs
		// Used to map in parallel, but reduce collectively
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/training.tsv"), TextInputFormat.class, RevisionMapper.class);
		MultipleInputs.addInputPath(configuration, new Path("/bmarcaur/data/titles.tsv"), TextInputFormat.class, ArticleMapper.class);
		
		// Set output for reduce job
		Calendar cal = Calendar.getInstance();
		FileOutputFormat.setOutputPath(configuration, new Path("/bmarcaur/data/articles/" + cal.get(Calendar.DATE) + '_' + cal.get(Calendar.HOUR) + '_' + cal.get(Calendar.MINUTE)));
		
		// Run the job
		JobClient.runJob(configuration);
	}		
}
