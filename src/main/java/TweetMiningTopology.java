import spout.TweetSpout;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class TweetMiningTopology {

	public static void main(String[] args) throws Exception {
		
		//create a new topology
		TopologyBuilder builder = new TopologyBuilder();

		//create a single TweetSpout we can only have one due to twitter's api req
		builder.setSpout("tweet", new TweetSpout(), 1);
		
		//print out each tweet
		builder.setBolt("print", new PrintBolt(), 5)
			.shuffleGrouping("tweet");
		
		builder.setBolt("process", new ProcessTweetBolt(), 5)
		.shuffleGrouping("tweet");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			//Utils.sleep(10000);
			//cluster.killTopology("test");
			//cluster.shutdown();
		}
	}
	
}
