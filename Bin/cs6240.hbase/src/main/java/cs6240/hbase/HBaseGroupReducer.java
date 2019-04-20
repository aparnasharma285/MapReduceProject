package cs6240.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HBaseGroupReducer extends Reducer<Text, Text, NullWritable, Put> {

	@Override
	public void reduce(final Text key, final Iterable<Text> groups, final Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		for (Text grp : groups) {
			sb.append(grp.toString());
			sb.append("-");
		}
		String formattedGroups = sb.substring(0, sb.length() - 1);
		byte[] row = Bytes.toBytes(key.toString());
		Put p = new Put(row);
		byte[] colFam = Bytes.toBytes("data");
		byte[] data = Bytes.toBytes(formattedGroups);
		//Cell cell = CellUtil.createCell(colFam, data);
		p.add(colFam, Bytes.toBytes("group"), data);
		context.write(null, p);
	}

}
