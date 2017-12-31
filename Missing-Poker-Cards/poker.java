import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class poker {
	public static class theMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		Text kyI = new Text();
		Text valU = new Text();

		@Override
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] ls = line.split("\t");
			this.kyI.set(ls[0]);
			this.valU.set(ls[1]);
			context.write(kyI, new IntWritable(Integer.parseInt(ls[1])));
		}
	}

	public static class theReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			ArrayList<Integer> chck = new ArrayList<Integer>();
			int count = 0, temp = 0;
			for (IntWritable x : value) {
				count = count + 1;
				temp = x.get();
				chck.add(temp);
			}

			if (count < 14) {
				for (int i = 1; i <= 13; i++) {
					if (!chck.contains(i))
                                          
						context.write(key, new IntWritable(i));
				}

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MissingCards");
		job.setJarByClass(poker.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(theMap.class);
		job.setReducerClass(theReducer.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}

