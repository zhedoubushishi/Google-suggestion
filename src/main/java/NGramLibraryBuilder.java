import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NGramLibraryBuilder {

	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		int noGram;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}
		
		//map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//key is offset, value is the content of one line(sentence)
			//context allows mapper/reducer interact with the rest of hadoop system(like global variable)
			//input: read sentence
			//I like eat n=3
            /*I like ->1
            like eat ->1
            I like eat ->1
            */
			String line = value.toString();
			line = line.trim().toLowerCase().replaceAll("[^a-z]+", " ");

			String words[] = line.split("\\s+");
			
			if(words.length < 2) {
				return;
			}
			
			StringBuilder sb;
			for (int i = 0; i < words.length-1; i++) {
				sb = new StringBuilder();
				sb.append(words[i]);

				for (int j = 1;  i + j < words.length && j < noGram; j++) {
					sb.append(" ");
					sb.append(words[i + j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
			}
		}
	}


	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		int threshold;   // if less than threshold, drop the line

		@Override
		public void setup(Context context) {
		    Configuration conf = context.getConfiguration();
		    threshold = conf.getInt("threshold", 10);
		}
		
		
		//reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			
			if (sum >= threshold) {
				context.write(key, new IntWritable(sum));
			}
		}
	}
}
