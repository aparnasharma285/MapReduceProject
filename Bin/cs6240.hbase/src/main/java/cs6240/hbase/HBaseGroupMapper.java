package cs6240.hbase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseGroupMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(final Object key, final Text value, final Context context)
			throws IOException, InterruptedException {
		String[] splitVals = value.toString().split(",");
		Text outKey = new Text(splitVals[0]);
		Text outVal = new Text(splitVals[1]);
		context.write(outKey, outVal);
	}
}
