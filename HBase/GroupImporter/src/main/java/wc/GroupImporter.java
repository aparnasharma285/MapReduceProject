package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GroupImporter extends Configured implements Tool {

    public static final String GROUP_DATA_TABLE = "UserGroupTable";
    public static final byte[] BYTE_GROUP_DATA_TABLE = Bytes.toBytes(GROUP_DATA_TABLE);
    public static final String DEFAULT_COLUMN_FAMILY = "data";
    public static final byte[] BYTE_DEFAULT_COLUMN_FAMILY = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
    public static final byte[] BYTE_GROUPS_QUALIFIER = Bytes.toBytes("groups");

    private static final Logger logger = LogManager.getLogger(GroupImporter.class);

    public static class FirstMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse the input string into a nice map
            String[] list = value.toString().split(",");
                context.write(new Text(list[0]),new Text(list[1]));
        }
    }


    public static class FirstReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        private ArrayList<Text> listFrom = new ArrayList<Text>();
        private ArrayList<Text> listTo = new ArrayList<Text>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            ArrayList<String> arr = new ArrayList<String>();
            for (Text val : values) {
                arr.add(val.toString());
            }
            String groups = arr.stream().collect(Collectors.joining("-"));
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(BYTE_DEFAULT_COLUMN_FAMILY, BYTE_GROUPS_QUALIFIER, Bytes.toBytes(groups));
            context.write(null, put);
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ec2-54-221-155-8.compute-1.amazonaws.com");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master","ec2-54-221-155-8.compute-1.amazonaws.com");
        conf.set("hbase.master.port","60000");
        conf.set("hbase.rootdir", "s3://justreduceit/hbase2/data");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        try {
            TableName tableName = TableName.valueOf(GroupImporter.GROUP_DATA_TABLE);
            HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
            hTableDesc.addFamily(new HColumnDescriptor(GroupImporter.DEFAULT_COLUMN_FAMILY));
            admin.createTable(hTableDesc);
            HTableDescriptor[] tables = admin.listTables();
            if (tables.length < 1) {
                throw new IOException("Failed create of table");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Job job = Job.getInstance(conf, "Hbase Group creation");
        job.setJarByClass(GroupImporter.class);
        job.setMapperClass(FirstMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(GroupImporter.GROUP_DATA_TABLE,
                FirstReducer.class, job);
        job.setReducerClass(FirstReducer.class);
        job.waitForCompletion(true);



        //job.waitForCompletion(true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        try {
            ToolRunner.run(new GroupImporter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


}