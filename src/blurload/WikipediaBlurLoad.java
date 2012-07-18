package blurload;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import blurload.mappers.BlurArticleMapper;
import blurload.mappers.BlurRevisionMapper;

import com.nearinfinity.blur.mapreduce.BlurTask;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

// Implements tool runner for argument parsing
public class WikipediaBlurLoad extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WikipediaBlurLoad(), args));
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// Remove my old table definition if it exists (optional)
		Blur.Iface client = BlurClient.getClient("nic-blurtop01:40010");
		if(client.tableList().contains("User Revisions")){
			client.disableTable("User Revisions");
			client.removeTable("User Revisions", true);
		}
		
		// Create the table definition using the standard analyzer and our systems defaults
		AnalyzerDefinition defaultAnalyzer = new AnalyzerDefinition();
		
		TableDescriptor userTable = new TableDescriptor();
		userTable.setShardCount(4);
		userTable.setTableUri("hdfs://nic-blurtop01:54310/bmarcaur/tables/user_revisions");
		userTable.setName("User Revisions");
		userTable.setAnalyzerDefinition(defaultAnalyzer);
		userTable.setCluster("default");

		// Create the blur map reduce task
		BlurTask loadUserRecords = new BlurTask();
		loadUserRecords.setTableDescriptor(userTable);
		
		// Set some job configurations
		// Get the configuration from the tool runner, expand jvm heap
		Configuration config = getConf();
		config.set("mapred.child.java.opts", "-Xmx2048m");
		
		Job userJob = loadUserRecords.configureJob(config);		
		userJob.setJobName("User Revision Load");
		userJob.setOutputFormatClass(TextOutputFormat.class);
		
		// Bind specific maps to particular inputs
		// Used to map in parallel, but reduce collectively
		MultipleInputs.addInputPath(userJob, new Path("/bmarcaur/data/revisions/1_17_10"), TextInputFormat.class, BlurRevisionMapper.class);
		MultipleInputs.addInputPath(userJob, new Path("/bmarcaur/data/articles/1_15_21"), TextInputFormat.class, BlurArticleMapper.class);

		// Output for your reduce task
		Calendar cal = Calendar.getInstance();
		FileOutputFormat.setOutputPath(userJob, new Path("/bmarcaur/data/load_jobs/blur_load_" + cal.get(Calendar.DATE) + '_' + cal.get(Calendar.HOUR) + '_' + cal.get(Calendar.MINUTE)));
		return (userJob.waitForCompletion(true) ? 0 : 1);
	}
}
