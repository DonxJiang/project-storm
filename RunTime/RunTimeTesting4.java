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



public class RunTimeTesting4 {
	public static class RandomSpoutOne extends BaseRichSpout{
	    private SpoutOutputCollector collector;
	    private String[] words = {"happy","excited","angry"};
	    
	    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
	        // TODO Auto-generated method stub
	        this.collector = arg2;
	    }
	    
	    public void nextTuple() {
	    	
	        String word = words[new Random().nextInt(words.length)]; 	        
//	        long startTime=System.nanoTime();
	        long startTime=System.currentTimeMillis();
//	        Utils.sleep(5);
	        collector.emit(new Values(word,startTime));
	    	
	    }
	    
	    public void declareOutputFields(OutputFieldsDeclarer arg0) {
	    	
	        arg0.declare(new Fields("randomstring","StartTime"));
	    }
	}
	
// SequenceBolt	
	public static class SenqueceBoltOne extends BaseBasicBolt{
	    public void execute(Tuple input, BasicOutputCollector collector) {
	        // TODO Auto-generated method stub
//	    	 long ReceivedTime = System.nanoTime();
	    	 long ReceivedTime = System.currentTimeMillis();

	         String word = (String) input.getValue(0);
	         
	         long StartTime=input.getLong(1);
	         	         
	    	 long TransmissionTime = ReceivedTime-StartTime;
	    	 System.out.println("TransmissionTime=" + TransmissionTime);
//	         long EndTime=System.nanoTime();
	         
	         long EndTime=System.currentTimeMillis();
	         long TotalDuration=EndTime-StartTime;
	         
	         
	         String out = "I'm " + word +  "!";  
	         System.out.println("out=" + out + TotalDuration);
	    }
	    
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        // TODO Auto-generated method stub
	    }
	}
	

    
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new RandomSpoutOne(), 3);
        
        builder.setBolt("bolt", new SenqueceBoltOne(), 1).shuffleGrouping("spout"); 
        Config conf = new Config();  
        conf.setDebug(false); 
        if (args != null && args.length > 0) {  
            conf.setNumWorkers(1);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("RandomDataStream", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("RandomDataStream");  
            cluster.shutdown();  
        }  
	
    }
}
