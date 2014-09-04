package spout;
import java.util.Map;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class TweetSpout implements IRichSpout {
	
	SpoutOutputCollector _collector;

	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	public void open(Map map, TopologyContext topCont, SpoutOutputCollector collector) {
		
		_collector = collector;
		
		// TODO Auto-generated method stub
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("3RmFowhS7CrDRKX4xevAHqkhY");
        cb.setOAuthConsumerSecret("hPzV8gCV2tvDSguQidD3o5OPXqldfyAc57Km2bo2hQWayGIcXQ");
        cb.setOAuthAccessToken("78093-g0OTZgKrr6BJaBQG6RdNZeCdNohgLHHoM1t77IMctGIT");
        cb.setOAuthAccessTokenSecret("NrZLDjxkYarUkmTKlpbPBCKBCYbBsgShyWM7lmC8B4ah3");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {
	        	//this is what happens when a status message comes in
	            //System.out.println(status.getUser().getName() + " : " + status.getText());
	        	_collector.emit(new Values(status));
	            
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
	    };
	    
	    twitterStream.addListener(listener);
	    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
	    
	    twitterStream.sample();
		

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
