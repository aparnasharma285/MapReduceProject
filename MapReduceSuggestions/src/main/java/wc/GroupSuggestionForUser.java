// Program to find the suitable groups a user can join using his friends list and  his friend's groups using Reduce Side Join
package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class GroupSuggestionForUser extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(GroupSuggestionForUser.class);

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
				friend.set("friend," + tokens[1]);
				context.write(user, friend); // emit the edges to show friendship between users
				user.set(tokens[1]);
				friend.set("friend," + tokens[0]);
				context.write(user, friend); // emit the edges to show friendship between users

				/*Since the data represents friendship between users
				 * which is bidirectional, hence we emit data in
				 * both directions.
				 */

		}
	}

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
            group.set("group," + tokens[1]);
            context.write(user, group); // emit the edges to show user and his group
        }
    }

    public static class SuggestionReaderMapper extends Mapper<Object, Text, Text, Text> {

        private final Text user = new Text();
        private final Text group = new Text();

        // Map function to get groups for each user
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); // split line by comma and get users and friend

            user.set(tokens[0]);
            group.set("suggestedGroup," + tokens[1]);
            context.write(user, group); // emit the edges to show user and his group
        }
    }

	public static class SuggestionReducer extends Reducer<Text, Text, Text, Text> {

		private List<Text> listOfFriends = new ArrayList<>();
		private List<Text> listOfSuggestedGroups = new ArrayList<>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear our lists
            listOfFriends.clear();
            listOfSuggestedGroups.clear();


            // save values in lists depending upon its values
			for (Text t : values) {
				String[] tokens = t.toString().split(",");
				if (tokens[0].equals("friend")) {
                    listOfFriends.add(new Text(tokens[1]));
				} else if (tokens[0].equals("group")) {
                    listOfSuggestedGroups.add(new Text(tokens[1]));
				}
			}

			emitSuggestions(context);
		}

		private void emitSuggestions(Context context) throws IOException, InterruptedException {

			// Identifies groups of friends and emits them as suggestions
			if (!listOfFriends.isEmpty() && !listOfSuggestedGroups.isEmpty()) {
			    // emit each user in listOfFriends with each member in the listOfSuggestedGroups as its suggested group
				for (Text friend : listOfFriends) {
					for (Text suggestedGroup : listOfSuggestedGroups) {
						context.write(friend, suggestedGroup);
					}
				}
			}
		}
	}


    public static class FinalSuggestionReducer extends Reducer<Text, Text, Text, Text> {

        private List<Text> listOfExistingGroups = new ArrayList<>();
        private List<Text> listOfSuggestedGroups = new ArrayList<>();
        private Text NO_SUGGESTION = new Text("No Suggestion Found");
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Clear our lists
            listOfExistingGroups.clear();
            listOfSuggestedGroups.clear();


            // save values in lists depending upon its values
            for (Text t : values) {
                String[] tokens = t.toString().split(",");
                if (tokens[0].equals("group")) {
                    listOfExistingGroups.add(new Text(tokens[1]));
                } else if (tokens[0].equals("suggestedGroup")) {
                    listOfSuggestedGroups.add(new Text(tokens[1]));
                }
            }

            emitFinalSuggestions(key, context);
        }

        private void emitFinalSuggestions(Text user, Context context) throws IOException, InterruptedException {

            List<Text> suggestedGroups = new ArrayList<>();

            if (!listOfExistingGroups.isEmpty() && !listOfSuggestedGroups.isEmpty()) {
                // emit each user in listOfFriends with each member in the listOfSuggestedGroups as its suggested group
                for (Text suggestedGroup : listOfSuggestedGroups) {
                    // check if user is not already in the suggested and and the suggestion is unique
                        if(!listOfExistingGroups.contains(suggestedGroup) && !suggestedGroups.contains(suggestedGroup)){
                            suggestedGroups.add(suggestedGroup);
                        }

                }
                // if user is not in any group then suggest all groups of his friends
            } else if (listOfExistingGroups.isEmpty() && !listOfSuggestedGroups.isEmpty()){
                for (Text suggestedGroup : listOfSuggestedGroups){
                    if(!listOfExistingGroups.contains(suggestedGroup) && !suggestedGroups.contains(suggestedGroup)){
                        suggestedGroups.add(suggestedGroup);
                    }
                    context.write(user,suggestedGroup);
                }
            } else {
                context.write(user, NO_SUGGESTION);
            }

            // emit all unique groups for user
            if(!suggestedGroups.isEmpty()){
                for(Text suggestedGroup: suggestedGroups){
                    context.write(user,suggestedGroup);
                }
            }
        }
    }

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Get Suggested Groups Job");

		job.setJarByClass(GroupSuggestionForUser.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// initial phase to read the input file and emit groups and friends
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,FriendsReaderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GroupsReaderMapper.class);
		job.setReducerClass(SuggestionReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

		final Job nextJob = Job.getInstance(conf, "Eliminate Duplicates and Already Enrolled Groups Job");

		// Intermediate phase to find groups of user's friends and suggest them to the user
		nextJob.setJarByClass(GroupSuggestionForUser.class);
        final Configuration nextJobConf = nextJob.getConfiguration();
        nextJobConf.set("mapreduce.output.textoutputformat.separator", ",");

        MultipleInputs.addInputPath(nextJob, new Path(args[2]), TextInputFormat.class, SuggestionReaderMapper.class);
        MultipleInputs.addInputPath(nextJob, new Path(args[1]), TextInputFormat.class, GroupsReaderMapper.class);
		nextJob.setReducerClass(FinalSuggestionReducer.class);

		nextJob.setOutputKeyClass(Text.class);
		nextJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(nextJob, new Path(args[3]));
		job.waitForCompletion(true);

		return nextJob.waitForCompletion(true) ? 0 : 1;

	}
    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<friend-dir> <group-dir> <intermediate-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new GroupSuggestionForUser(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}

