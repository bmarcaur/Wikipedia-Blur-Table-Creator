package test_scripts;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public class RunQuery {
	public static void main(String[] args) throws BlurException, TException {
		Blur.Iface client = BlurClient.getClient("nic-blurtop01:40010");
		BlurQuery blurQuery = new BlurQuery();
		SimpleQuery simpleQuery = new SimpleQuery();
		simpleQuery.setQueryStr("revisions.article_id:10000029");
		blurQuery.setSimpleQuery(simpleQuery);
		blurQuery.setSelector(new Selector());

		BlurResults blurResults = client.query("User Revisions", blurQuery);
		for (BlurResult result : blurResults.getResults()) {
		   String test = result.toString();
		   test.toString();
		}
	}
}
