package cs6240.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HBaseDriver extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(HBaseDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration hConf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(hConf);

		Admin admin = connection.getAdmin();
		TableName tName = TableName.valueOf("groups");
		HTableDescriptor htd = new HTableDescriptor(tName);
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);

		Table table = connection.getTable(tName);
		Job job = Job.getInstance(getConf(), "Hbase Groups");

		job.setJarByClass(HBaseDriver.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "groups");
		job.setMapperClass(HBaseGroupMapper.class);
		job.setReducerClass(HBaseGroupReducer.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		int status = 0;
		if (job.waitForCompletion(true)) {
			table.close();
		} else {
			status = 1;
		}
		return status;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Expected input as <input>");
		}
		try {
			ToolRunner.run(new HBaseDriver(), args);
		} catch (Exception e) {
			logger.info(" " + e.getMessage());
		}
	}
}
