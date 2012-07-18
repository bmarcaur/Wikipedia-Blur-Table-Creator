package blurload.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.nearinfinity.blur.mapreduce.BlurMapper;
import com.nearinfinity.blur.mapreduce.BlurMutate.MUTATE_TYPE;
import com.nearinfinity.blur.mapreduce.BlurRecord;

public class BlurRevisionMapper extends BlurMapper<LongWritable, Text> {
	private static BlurRecord record;
	private static String[] columnNames = { "revision_id", "user_id",
			"article_id", "revision_id", "namespace", "timestamp", "md5",
			"reverted", "reverted_user_id", "reverted_revision_id",
			"delta", "cur_size", "comment" };
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		record = _mutate.getRecord();
		_mutate.setMutateType(MUTATE_TYPE.ADD);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columnValues = value.toString().split("\\t");

		// Reset record
		record.clearColumns();
		record.setRowId(columnValues[1]);
		record.setRecordId(columnValues[0]);
		record.setFamily("revisions");

		// Grab and load the column values
		for (int index = 1; index < columnNames.length; index++) {
			if (index < columnValues.length){
				record.addColumn(columnNames[index], columnValues[index]);
			}
		}

		// Set the key (usually the rowid)
		byte[] bs = record.getRowId().getBytes();
		_key.set(bs, 0, bs.length);
		
		context.write(_key, _mutate);
		_recordCounter.increment(1);
		context.progress();
	}
}
