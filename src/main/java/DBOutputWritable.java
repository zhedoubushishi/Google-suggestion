import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;


public class DBOutputWritable implements DBWritable {

	private String starting_phrase;
	private String following_word;
	private int count;

	public DBOutputWritable(String starting_phrase, String following_word, int count) {
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.count = count;
	}

	public void readFields(ResultSet arg) throws SQLException {
		starting_phrase = arg.getString(1);
		following_word = arg.getString(2);
		count = arg.getInt(3);
	}

	public void write(PreparedStatement arg) throws SQLException {
		arg.setString(1, starting_phrase);
		arg.setString(2, following_word);
		arg.setInt(3, count);
	}
}