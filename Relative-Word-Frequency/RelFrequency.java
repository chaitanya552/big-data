import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.TreeSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;


public class RelFrequency {

	public static TreeSet<OutputPair> sortedOutput = new TreeSet<>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] elements = value.toString().split("\\s+");
			for (String word : elements) {
				if (word.matches("^\\w+$")) {
					//to differentiate a word and the word with neighbour
					context.write(new Text(word.trim() + " " + "#"), new LongWritable(1));
					// stored in the mapper as 
					// word1 #,1
					// word2 #,1
				}
			}
			StringBuilder sb = new StringBuilder();
			for (int index = 0; index < elements.length -1; index++) {
					if (elements[index].matches("^\\w+$") && elements[index + 1].matches("^\\w+$")) {
						sb.append(elements[index]).append(" ").append(elements[index + 1]);
						context.write(new Text(sb.toString()), new LongWritable(1));
						//stored in the mapper as
						// word1 nextWord,1
						// word2 nextWord,1
						sb.delete(0, sb.length());
				
				}
			}

		}

	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private DoubleWritable sumCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private Text currentElement = new Text("B-L-A-N-K / E-M-P-T-Y");

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			String[] keyComp = key.toString().split(" ");
			if (keyComp[1].equals("#")) {
				if (keyComp[0].equals(currentElement.toString())) {
					sumCount.set(sumCount.get() + fetchSumCount(values));
				} else {
					currentElement.set(keyComp[0]);
					sumCount.set(0);
					sumCount.set(fetchSumCount(values));
				}
			} else {

				double count = fetchSumCount(values);
				if (count != 1) {
					relativeCount.set((double) count / sumCount.get());
					Double relativeCountD = relativeCount.get();
					
					if(relativeCountD ==1.0d){

					sortedOutput.add(new OutputPair(relativeCountD,count, key.toString(), currentElement.toString()));

					if (sortedOutput.size() > 100) {
						sortedOutput.pollLast();
					}

					context.write(key, new Text(Double.toString(relativeCountD)));}
				}
			}
		}

		private double fetchSumCount(Iterable<LongWritable> values) {
			double count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			return count;
		}

	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(RelFrequency.class);
        job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);

		File file1 = new File(args[1] + "/top100.txt");
		file1.createNewFile();
		FileWriter fw = new FileWriter(file1);
		for (OutputPair now : sortedOutput) {
			fw.write("word and its neighbour ->  " +now.key + " -> Main word ->  " + now.value + "    its relative frequency-> " + now.relativeFrequency + "\n");
		}
		fw.close();


	}

	public static class OutputPair implements Comparable<OutputPair> {
		double relativeFrequency;
		double count;
		String key;
		String value;

		OutputPair(double relativeFrequency, double count, String key, String value) {
			this.relativeFrequency = relativeFrequency;
			this.count = count;
			this.key = key;
			this.value = value;
		}

		@Override
		public int compareTo(OutputPair outputPair) {
			
				if(this.count<=outputPair.count){
				
				return 1;
			} else {
				return -1;
			}
			
		}
	}

}



