import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// args[0]: input directory
		// args[1]: output directory
		// args[2]: noGram
		// args[3]: threshold
		// args[4]: topK

		//job1
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", ".");
		conf1.set("noGram", args[2]);

	    Job job1 = Job.getInstance(conf1);
	    job1.setJobName("NGram");
	    job1.setJarByClass(Driver.class);

	    job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
	    job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));

	    job1.waitForCompletion(true);

	    //job2
	    Configuration conf2 = new Configuration();
	    conf2.set("threshold", args[3]);
	    conf2.set("topK", args[4]);
	    DBConfiguration.configureDB(conf2,
	    	     "com.mysql.jdbc.Driver",   // driver class
	    	     "jdbc:mysql://192.168.109.140:3306/test", // database url
	    	     "root",    // user name
	    	     "123"); //password
		
	    Job job2 = Job.getInstance(conf2);
	    job2.setJobName("LanguageModel");
	    job2.setJarByClass(Driver.class);

	    // add dependency
	    job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);

	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(DBOutputWritable.class);	  // reducer
	    job2.setOutputValueClass(NullWritable.class);

	    job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);

		//Path name for this job should match first job's output path name
		TextInputFormat.setInputPaths(job2, args[1]);
		String[] table_col = new String[] {"starting_phrase", "following_word", "count"};
 		DBOutputFormat.setOutput(
			     job2,
			     "output",    // table name
				 table_col   		    //table columns
			     );

		job2.waitForCompletion(true);

	}
}
