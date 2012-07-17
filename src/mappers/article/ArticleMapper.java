package mappers.article;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ArticleMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>  {
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
