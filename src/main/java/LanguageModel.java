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

		int threshold;   // if less than threshold, drop the line
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 10);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input value: I like eat apple\t10
			String[] line = value.toString().trim().split("\t");

			String[] words = line[0].split("\\s+");   // words: {I, like, eat, apple}
			int count = Integer.parseInt(line[1]);

			if ((line.length < 2) || (count < threshold)) {
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

		/*
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//this -> <is=1000, is book=10>
			
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text val : values) {
				String cur_val = val.toString().trim();
				String word = cur_val.split("=")[0].trim();
				int count = Integer.parseInt(cur_val.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iter = tm.keySet().iterator();
			
			for(int j=0 ; iter.hasNext() && j < n; j++) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for(String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
					j++;
				}
			}
		}
		*/
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
			public int compare(wordCountPair a, wordCountPair b) {
				return a.count - b.count;
			}

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			PriorityQueue<wordCountPair> queue = new PriorityQueue<wordCountPair>();
			// get out of the last words
			for (Text value: values) {
				String temp = value.toString().trim();
				String[] wordAndCount = temp.split("=");
				//String word = wordAndCount[0].trim();
				//int count = Integer.parseInt(wordAndCount[1]);
				wordCountPair pair = new wordCountPair(Integer.parseInt(wordAndCount[1]), wordAndCount[0].trim());
				queue.add(pair);
			}
			// put into database
			for (int i = 0; i < topK && queue.size() > 0; i++) {
				 wordCountPair temp = queue.poll();
				 context.write(new DBOutputWritable(key.toString(), temp.word, temp.count), NullWritable.get());
			}
		}
	}
}
