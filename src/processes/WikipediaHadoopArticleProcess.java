package processes;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
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

public class WikipediaHadoopArticleProcess {
	public static void main(String[] args) throws IOException {
		JobConf configuration = new JobConf(WikipediaHadoopArticleProcess.class);
		configuration.setJobName("Wikipedia Articles");
		
		configuration.setOutputKeyClass(IntWritable.class);
		configuration.setOutputValueClass(Text.class);
		
		configuration.setReducerClass(Reduce.class);
		
		MultipleInputs.addInputPath(configuration, new Path("/data/bmarcaur/wikidata/" + args[0]), TextInputFormat.class, RevisionMapper.class);
		MultipleInputs.addInputPath(configuration, new Path("/data/bmarcaur/wikidata/" + args[1]), TextInputFormat.class, ArticleMapper.class);
		
		configuration.setOutputFormat(TextOutputFormat.class);
		Date today = new Date();
		FileOutputFormat.setOutputPath(configuration, new Path("/data/bmarcaur/wikioutput/articles/" + today.getDay() + '_' + today.getHours() + '_' + today.getMinutes()));
		
		JobClient.runJob(configuration);
	}
	
	public static class RevisionMapper extends MapReduceBase  implements Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable articleId = new IntWritable();
		private Text userId = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			try{
				String columns = value.toString();
				String rawArticleId = columns.split("\\t")[1];
				articleId.set(Integer.parseInt(rawArticleId));
				userId.set(columns.split("\\t")[0]);
				output.collect(articleId, userId);
			} catch (NumberFormatException e){}
		}
	}
	
	public static class ArticleMapper extends MapReduceBase  implements Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable articleId = new IntWritable();
		private Text combinedColumns = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			try{
				String columns = value.toString();
				String rawArticleId = columns.split("\\t")[0];
				articleId.set(Integer.parseInt(rawArticleId));
				combinedColumns.set(columns);
				output.collect(articleId, combinedColumns);
			} catch (NumberFormatException e){}
		}
	}
		
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String articleInfo = new String();
			HashSet<String> userIds = new HashSet<String>();
			while(values.hasNext()){
				String columns = values.next().toString();
				if(columns.split("\\t").length < 3){
					userIds.add(columns);
				} else {
					articleInfo = columns;
				}
			}
			
			if(userIds.size() > 0){
				String combinedUserIds = new String();
				for(String userId : userIds){
					combinedUserIds += userId + '|';
				}
				combinedUserIds = combinedUserIds.substring(0, combinedUserIds.length() - 2);
				articleInfo += combinedUserIds;
				output.collect(key, new Text(articleInfo));
			}	
		}
	}
}
