import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.io.IOException;


public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/** 
		int threshold;   // if less than threshold, drop the line
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 10);
		}
		*/

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input value: I like eat apple\t20
			String[] line = value.toString().trim().split("\t");

			String[] words = line[0].split("\\s+");   // words: {I, like, eat, apple}
			int count = Integer.parseInt(line[1]);

			if ((line.length < 2)) {
				return;
			}

			// output key and value
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}

			String outputKey = sb.toString().trim();    //outputKey: "I like eat"

			context.write(new Text(outputKey), new Text(words[words.length - 1] + "=" + count));
		}
	}


	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", 5);
		}

		private class wordCountPair implements Comparable<wordCountPair> {
			int count;
			String word;

			public wordCountPair(int count, String word) {
				this.count = count;
				this.word = word;
			}

			public int compareTo(wordCountPair a) {
				return this.count - a.count;
			}
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			PriorityQueue<wordCountPair> queue = new PriorityQueue<wordCountPair>();
			
			for (Text value: values) {
				String temp = value.toString().trim();
				String[] wordAndCount = temp.split("=");

				wordCountPair pair = new wordCountPair(Integer.parseInt(wordAndCount[1]), wordAndCount[0]);
				queue.add(pair);
			}
			// put into database
			for (int i = 0; i < topK && i < queue.size(); i++) {
				 wordCountPair temp = queue.poll();
				 context.write(new DBOutputWritable(key.toString(), temp.word, temp.count), NullWritable.get());
			}
		}
	}
}
