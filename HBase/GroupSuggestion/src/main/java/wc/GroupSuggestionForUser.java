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

    public static final String GROUP_DATA_TABLE = "UserGroupTable";
    public static final String DEFAULT_COLUMN_FAMILY = "data";
    public static final String GROUPS_QUALIFIER = "groups";

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
            /* Since the data represents friendship between users
             * which is bidirectional, hence we emit data in both directions.
             */
            context.write(user, friend); // emit the edges to show friendship between users
            context.write(friend,user); // emit the edges to show friendship between users
        }
    }

    public static class SuggestionReducer extends Reducer<Text, Text, Text, Text> {

        Table table;
        byte[] columnNFamily;
        byte[] columnName;
        List<String> listOfUserGroups = new ArrayList<>();

        /**
         * Setup to run before all reduce tasks
         * @param context
         * @throws IOException
         */
        protected void setup(Context context) throws IOException{
            /**
             * Get configuration and other needed values and incase set zookeeper quorum.
             */
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("tableName");
            Connection connection = ConnectionFactory.createConnection(conf);
            conf.set("hbase.zookeeper.quorum", "ec2-54-236-58-85.compute-1.amazonaws.com");
            conf.set("hbase.zookeeper.property.clientPort","2181");

            table = connection.getTable(TableName.valueOf(tableName));
            String colfam = conf.get("columnFamily");
            columnNFamily = Bytes.toBytes(colfam);
            String colName = conf.get("columnName");
            columnName = Bytes.toBytes(colName);
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**
             * Gets List of groups in which user is already member and also find list of all given user's friend's groups.
             * Then, suggest given user with friend's groups in which user is not member of
             *
             * For group retrieval, it uses helper function getGroupsFromTable
             */
            listOfUserGroups = getGroupsFromTable(table,key.toString(),columnNFamily, columnName);
            List<String> listOfSuggestedGroups = new ArrayList<>();

            for(Text friend: values){
                System.out.println("Key:"+key.toString()+" friend:"+friend.toString());
                List<String> listOfFriendGroups = getGroupsFromTable(table,friend.toString(),columnNFamily,columnName);
                for(String group:listOfFriendGroups){
                    if(!listOfUserGroups.contains(group) && !listOfSuggestedGroups.contains(group)){
                        listOfSuggestedGroups.add(group);
                    }
                }
            }

            // emits all suggested groups
            for (String group: listOfSuggestedGroups){
                context.write(key,new Text(group));
            }
        }

        /**
         *
         * @param table hbase client table object where to find keyToFind
         * @param keyToFind rowkey to find
         * @param columnNFamily column family in which we need to find rowKey
         * @param columnName column name which contains group's data
         * @return list of users group
         * @throws IOException
         * @throws InterruptedException
         */
        private List<String> getGroupsFromTable(Table table, String keyToFind, byte[] columnNFamily, byte[] columnName) throws IOException, InterruptedException {
            // creates get query
            Get get = new Get(Bytes.toBytes(keyToFind));
            get.addFamily(columnNFamily);
            Result res = table.get(get); // gets value from hbase
            if (res != null) {
                Map<String, String> resultMap = new LinkedHashMap<>();
                resultMap.put(Bytes.toString(columnNFamily), getValue(res, columnNFamily,columnName )); // store value in Map
                for (Map.Entry<String, String> list : resultMap.entrySet()) {
                    String groups = list.getValue().trim();
                    // if group dash seperated list exist, then convert it to array list and return it
                    if(groups.length()==0){
                        return new ArrayList<>();
                    }
                    String[] groupList = groups.split("-");

                    return Arrays.asList(groupList);
                }
            }
            return new ArrayList<>();
        }

        private static String getValue(Result res, byte[] cf, byte[] qualifier) {
            byte[] value = res.getValue(cf, qualifier);
            return value == null? "": Bytes.toString(value);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job nextJob = Job.getInstance(conf, "Get Suggested List");
        final Configuration nextJobConf = nextJob.getConfiguration();
        nextJobConf.set("tableName",GroupSuggestionForUser.GROUP_DATA_TABLE);
        nextJobConf.set("columnFamily",GroupSuggestionForUser.DEFAULT_COLUMN_FAMILY);
        nextJobConf.set("columnName",GroupSuggestionForUser.GROUPS_QUALIFIER);
        nextJob.setJarByClass(GroupSuggestionForUser.class);
        nextJobConf.set("mapreduce.output.textoutputformat.separator", ",");
        nextJob.setMapperClass(FriendsReaderMapper.class);
        FileInputFormat.setInputPaths(nextJob, new Path(args[0]));
        nextJob.setReducerClass(SuggestionReducer.class);
        nextJob.setOutputKeyClass(Text.class);
        nextJob.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(nextJob, new Path(args[1]));

        return nextJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<friend-dir> <output-dir>");
        }

        try {
            final Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "ec2-54-236-58-85.compute-1.amazonaws.com");
            config.set("hbase.zookeeper.property.clientPort","2181");
            ToolRunner.run(config, new GroupSuggestionForUser(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}