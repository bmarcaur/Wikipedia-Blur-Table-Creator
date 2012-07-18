package articles.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ArticleReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
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
