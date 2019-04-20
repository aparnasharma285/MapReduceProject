package wc;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 *  This is testing code to find by key.
 *  Inspired by Hadoop The Definitive Guide by OReilly
 */
public class FindByKey extends Configured implements Tool {


    public static final String GROUP_DATA_TABLE = "UserGroupTable";
    public static final String DEFAULT_COLUMN_FAMILY = "data";
    public static final byte[] BYTE_DEFAULT_COLUMN_FAMILY = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
    public static final byte[] BYTE_GROUPS_QUALIFIER = Bytes.toBytes("groups");

    public Map<String, String> findByKeyHelper(Table table, String keyToFind)
            throws IOException {
        Get get = new Get(Bytes.toBytes(keyToFind));
        get.addFamily(FindByKey.BYTE_DEFAULT_COLUMN_FAMILY);
        Result res = table.get(get);
        if (res == null) {
            System.out.println("couldnt find the key");
            return null;
        }
        Map<String, String> resultMap = new LinkedHashMap<String, String>();
        resultMap.put(FindByKey.DEFAULT_COLUMN_FAMILY, getValue(res, FindByKey.BYTE_DEFAULT_COLUMN_FAMILY, FindByKey.BYTE_GROUPS_QUALIFIER));
        return resultMap;
    }

    private static String getValue(Result res, byte[] cf, byte[] qualifier) {
        byte[] value = res.getValue(cf, qualifier);
        return value == null? "": Bytes.toString(value);
    }

    public int run(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "ec2-54-236-58-85.compute-1.amazonaws.com");
        config.set("hbase.zookeeper.property.clientPort","2181");

        Connection connection = ConnectionFactory.createConnection(config);
        try {
            Table table = connection.getTable(TableName.valueOf(FindByKey.GROUP_DATA_TABLE));
            try {
                Map<String, String> fetchedInfo = findByKeyHelper(table, "");
                if (fetchedInfo == null) {
                    System.err.printf("ID %s not found.\n", "8286977");
                    return -1;
                }
                for (Map.Entry<String, String> info : fetchedInfo.entrySet()) {
                    System.out.printf("%s\t%s\n", info.getKey(), info.getValue());
                }
                return 0;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new FindByKey(), args);
        System.exit(exitCode);
    }
}