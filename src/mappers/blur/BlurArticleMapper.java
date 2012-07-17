package mappers.blur;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.nearinfinity.blur.mapreduce.BlurMapper;
import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.mapreduce.BlurMutate.MUTATE_TYPE;

public class BlurArticleMapper extends BlurMapper<LongWritable, Text> {
	private static BlurRecord record;
	private static String[] columnNames = { "article_id", "article_id", "category",
			"timestamp", "namespace", "redirect", "title", "related_page",
			"user_ids"};
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		record = _mutate.getRecord();
		_mutate.setMutateType(MUTATE_TYPE.ADD);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columnValues = value.toString().split("\\t");
		String[] userIds = columnValues[7].split("|");
		
		record.setFamily("articles");
		for (String userId : userIds){
			record.clearColumns();
			record.setRowId(userId);
			record.setRecordId(columnValues[0]);
			
			for (int index = 1; index < columnNames.length; index++) {
				if (index < columnValues.length){
					record.addColumn(columnNames[index], columnValues[index]);
				}
			}
			
			byte[] bs = record.getRowId().getBytes();
			_key.set(bs, 0, bs.length);
			context.write(_key, _mutate);
			_recordCounter.increment(1);
		}
		
		context.progress();
	}
}
