// Program to find the suitable groups a user can join using his friends list and  his friend's groups using Hbase and mapreduce 
package wc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
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


public class GroupSuggestionForUser extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(GroupSuggestionForUser.class);

    public static class GroupsReaderMapper extends Mapper<Object, Text, Text, Text> {

        private final Text user = new Text();
        private final Text group = new Text();

        // Map function to get groups for each user
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); // split line by comma and get users and friend

            user.set(tokens[0]);
            group.set(tokens[1]);
            context.write(user, group); // emit the edges to show user and his group
        }
    }


    public static class HbaseDataEntryReducer extends Reducer<Text, Text,  NullWritable, Put> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

           StringBuilder groups= new StringBuilder();
           for (Text value:values){
                groups.append(value.toString()+",");
           }

            byte[] rowKey = Bytes.toBytes(key.toString());
            byte[] value = Bytes.toBytes(groups.toString());
            byte[] qualifier = Bytes.toBytes("data");
            Put put = new Put(rowKey);
            byte[] columnName = Bytes.toBytes("groups");

            put.add(qualifier,columnName,value);
            context.write(null,put);
        }
    }


    public static class FriendsReaderMapper extends Mapper<Object, Text, Text, Text> {

        private final Text user = new Text();
        private final Text friend = new Text();

        // Map function to get friends for each user
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); // split line by comma and get users and friend

            user.set(tokens[0]);
            friend.set(tokens[1]);
            context.write(user, friend); // emit the edges to show friendship between users
            context.write(friend,user); // emit the edges to show friendship between users

            /*Since the data represents friendship between users
             * which is bidirectional, hence we emit data in
             * both directions.
             */

        }
    }

    public static class SuggestionReducer extends Reducer<Text, Text, Text, Text> {

        Table table;
        byte[] columnNFamily;
        byte[] columnName;
        List<String> listOfUserGroups = new ArrayList<>();

        protected void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("tableName");
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            String colfam = conf.get("columnFamily");
            columnNFamily = Bytes.toBytes(colfam);
            String colName = conf.get("columnName");
            columnName = Bytes.toBytes(colName);

        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            listOfUserGroups = getGroupsFromTable(table,key.toString(),columnNFamily, columnName);
            List<String> listOfSuggestedGroups = new ArrayList<>();

            for(Text friend: values){
               List<String> listOfFriendGroups = getGroupsFromTable(table,friend.toString(),columnNFamily,columnName);
               for(String group:listOfFriendGroups){
                   if(!listOfUserGroups.contains(group) && !listOfSuggestedGroups.contains(group)){
                       listOfSuggestedGroups.add(group);
                   }
               }

            }

            for (String group: listOfSuggestedGroups){
                context.write(key,new Text(group));
            }


        }

        private List<String> getGroupsFromTable(Table table, String keyToFind, byte[] columnNFamily, byte[] columnName) throws IOException, InterruptedException {

            Get get = new Get(Bytes.toBytes(keyToFind));
            get.addFamily(columnNFamily);
            Result res = table.get(get);
            if (res == null) {
                return null;
            }
            Map<String, String> resultMap = new LinkedHashMap<>();
            resultMap.put(Bytes.toString(columnNFamily), getValue(res, columnNFamily,columnName ));

            for (Map.Entry<String, String> list : resultMap.entrySet()) {
                String[] groupList = list.getValue().split(",");
                List<String> result = Arrays.asList(groupList);
                return result;
            }

            return null;
        }

        private static String getValue(Result res, byte[] cf, byte[] qualifier) {
            byte[] value = res.getValue(cf, qualifier);
            return value == null? "": Bytes.toString(value);
        }
    }

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Store User Groups In Hbase");

		job.setJarByClass(GroupSuggestionForUser.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "UserGroupTable");
		// initial phase to read the input file and save groups
        job.setMapperClass(GroupsReaderMapper.class);
        job.setReducerClass(HbaseDataEntryReducer.class);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]));
		job.waitForCompletion(true);

        final Job nextJob = Job.getInstance(conf, "Get Suggested List");
        final Configuration nextJobConf = nextJob.getConfiguration();
        nextJobConf.set("tableName","UserGroupTable");
        nextJobConf.set("columnFamily","data");
        nextJobConf.set("columnName","groups");

        // Intermediate phase to find groups of user's friends and suggest them to the user
        nextJob.setJarByClass(GroupSuggestionForUser.class);
        nextJobConf.set("mapreduce.output.textoutputformat.separator", ",");

        nextJob.setMapperClass(FriendsReaderMapper.class);
        FileInputFormat.setInputPaths(nextJob, new Path(args[0]));
        nextJob.setReducerClass(SuggestionReducer .class);
        nextJob.setOutputKeyClass(Text.class);
        nextJob.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(nextJob, new Path(args[2]));
        nextJob.waitForCompletion(true);


        return nextJob.waitForCompletion(true) ? 0 : 1;

	}
    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Four arguments required:\n<friend-dir> <group-dir> <output-dir>");
        }

        Configuration config;
        Connection connection = null;
        Admin admin = null;
        try {
            config = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(config);
            // Create table
            admin = connection.getAdmin();

                TableName tableName = TableName.valueOf("UserGroupTable");
                HTableDescriptor htd = new HTableDescriptor(tableName);
                HColumnDescriptor hcd = new HColumnDescriptor("data");
                htd.addFamily(hcd);

                if (admin.tableExists(tableName)){
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
                admin.createTable(htd);
        } catch (IOException e) {
            Logger.getLogger(e.getMessage());
        } finally {
            try {
                admin.close();
                connection.close();
            } catch (IOException e) {
              Logger.getLogger(e.getMessage());
            }
        }

        try {
            ToolRunner.run(new GroupSuggestionForUser(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
