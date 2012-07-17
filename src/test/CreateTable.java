package test;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class CreateTable {
	public static void main(String[] args) throws BlurException, TException {
		Blur.Iface client = BlurClient.getClient("nic-blurtop01:40010");
		AnalyzerDefinition ad = new AnalyzerDefinition();

		TableDescriptor td = new TableDescriptor();
		td.setShardCount(4);
		td.setTableUri("hdfs://nic-blurtop01:54310/bmarcaur/tables/user_revisions");
		td.setAnalyzerDefinition(ad);
		td.setName("User Revisions");
		td.setCluster("default");

		client.createTable(td);
	}

}
