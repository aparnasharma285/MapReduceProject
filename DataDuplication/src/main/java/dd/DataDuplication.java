package dd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class DataDuplication extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(DataDuplication.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] splitList = value.toString().split("-");
            context.write(new Text(splitList[0]), new Text(splitList[1]));
        }
    }

    public static class DuplicationReducer extends Reducer<Text, Text, Text, Text> {

        private final long maxNodeId = 1138499;
        private final long multiplyData = 10;

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            long keyLong = Long.parseLong(key.toString());
            for (final Text val : values) {
                long valLong = Long.parseLong(val.toString());
                for(int i=0; i <multiplyData; i++) {
                    context.write(new Text(Long.toString(keyLong+maxNodeId*i)), new Text(Long.toString(valLong+maxNodeId*i)));
                }
            }
        }
    }



    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Data Duplication");
        job.setJarByClass(DataDuplication.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DuplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new DataDuplication(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}