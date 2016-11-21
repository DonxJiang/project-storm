package RunTime;

import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class RunTimeTesting {
	public static class RandomSpoutOne extends BaseRichSpout{
	    private SpoutOutputCollector collector;
	    private String[] words = {"happy","excited","angry"};
	    
	    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
	        // TODO Auto-generated method stub
	        this.collector = arg2;
	    }
	    
	    public void nextTuple() {
	        // TODO Auto-generated method stub
	        String word = words[new Random().nextInt(words.length)]; 
	        collector.emit(new Values(word));
	    }
	    
	    public void declareOutputFields(OutputFieldsDeclarer arg0) {
	        // TODO Auto-generated method stub
	        arg0.declare(new Fields("randomstring"));
	    }
	}
	
// SequenceBolt	
	public static class SenqueceBoltOne extends BaseBasicBolt{
	    public void execute(Tuple input, BasicOutputCollector collector) {
	        // TODO Auto-generated method stub
	         String word = (String) input.getValue(0);  
	         String out = "I'm " + word +  "!";  
	         System.out.println("out=" + out);
	    }
	    
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        // TODO Auto-generated method stub
	    }
	}
	

    
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new RandomSpoutOne());
        
        builder.setBolt("bolt", new SenqueceBoltOne()).shuffleGrouping("spout"); 
        Config conf = new Config();  
        conf.setDebug(false); 
        if (args != null && args.length > 0) {  
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
        }  
	
    }
}
