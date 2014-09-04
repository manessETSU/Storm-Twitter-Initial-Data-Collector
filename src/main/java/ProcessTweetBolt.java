import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


public class ProcessTweetBolt implements IRichBolt {

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Status tweet = (Status) tuple.getValue(0);

		String dbUrl = "jdbc:mysql://localhost:8889/tweetDatabase";
		String dbClass = "com.mysql.jdbc.Driver";
		String username = "root";
		String password = "root";
		String query = "INSERT INTO `tweetDatabase`.`storeTweets` (`id`, `user`, `text`, `lang`) " +
					   "VALUES (NULL, ?, ?, ?)";
		try {

			Class.forName(dbClass);
			Connection connection = (Connection) DriverManager.getConnection(dbUrl,
					username, password);
			
			PreparedStatement statement = (PreparedStatement) connection.prepareStatement(query);
			
			statement.setString(1, tweet.getUser().getName());
			statement.setString(2, tweet.getText());
			statement.setString(3, tweet.getLang());
			
			statement.executeUpdate();
			
			connection.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
