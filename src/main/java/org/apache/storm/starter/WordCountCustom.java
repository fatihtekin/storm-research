package org.apache.storm.starter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WordCountCustom{

    private final static Logger LOG = LoggerFactory.getLogger(WordCountCustom.class);

    private static final String SENTENCE_STREAM = "sentenceStream";
    private static final String END_OF_DATA_STREAM = "EndOfDataStream";
    private static final String END_OF_DATA_STREAM2 = "EndOfDataStream2";
	private static final String WORD_STREAM = "wordStream";
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static class SentenceSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		private String[] sentences = {
				"my dog has fleas",
				"i like cold beverages",
				"the dog ate my homework",
				"don't have a cow man",
				"i don't think i like fleas"
		};
		private int index = 0;
		private int count = 0;
		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream(SENTENCE_STREAM,new Fields("sentence"));
			declarer.declareStream(END_OF_DATA_STREAM,new Fields("finished"));
		}
		public void open(Map config, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
		}
		public void nextTuple() {
			LOG.info("Spout is emitting");
			this.collector.emit(SENTENCE_STREAM,new Values(sentences[index]));
			index++;
			if (index >= sentences.length) {
				index = 0;
			}
			Utils.sleep(100);
			count++;
			if(count%10 == 0){
				this.collector.emit(END_OF_DATA_STREAM,new Values("EOD"));				
				Utils.sleep(5000);
			}
		}
	}
	
	public static class SplitSentenceBolt extends BaseRichBolt{
		private OutputCollector collector;
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream(WORD_STREAM,new Fields("word"));
			declarer.declareStream(END_OF_DATA_STREAM2,new Fields("finished"));
		}
		public void prepare(Map config, TopologyContext context,OutputCollector collector) {
			this.collector = collector;
		}
		public void execute(Tuple tuple) {
			if(tuple.getSourceStreamId().equals(END_OF_DATA_STREAM)){
				String eod = tuple.getStringByField("finished");	
				this.collector.emit(END_OF_DATA_STREAM2,new Values("EOD"));
			}else{				
				String sentence = tuple.getStringByField("sentence");
				String[] words = sentence.split(" ");
				for(String word : words){
					this.collector.emit(WORD_STREAM,new Values(word));
				}
			}
		}
	}
	
	public static class WordCountBolt extends BaseRichBolt{
		private OutputCollector collector;
		private HashMap<String, Long> counts = null;
		public void prepare(Map config, TopologyContext context,OutputCollector collector) {
			this.collector = collector;
			this.counts = new HashMap<String, Long>();
		}
		public void execute(Tuple tuple) {
			if(tuple.getSourceStreamId().equals(END_OF_DATA_STREAM2)){
				String eod = tuple.getStringByField("finished");	
				List<String> keys = new ArrayList<String>();
				keys.addAll(this.counts.keySet());
				Collections.sort(keys);
				StringBuilder sum = new StringBuilder();
				for (String key : keys) {
					sum.append(key + " : " + this.counts.get(key)+"\n");
				}
				LOG.info("--- FINAL COUNTS for worker id:"+"---\n"+sum.toString()+"--------------");
				counts.clear();
			}else{				
				String word = tuple.getStringByField("word");
				Long count = this.counts.get(word);
				if(count == null){
					count = 0L;
				}
				count++;
				this.counts.put(word, count);
			}
		}
		public void declareOutputFields(OutputFieldsDeclarer declarer){}		
	}	
	

	public static void main(String[] args) throws Exception {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID,SENTENCE_STREAM).allGrouping(SENTENCE_SPOUT_ID, END_OF_DATA_STREAM);
		builder.setBolt(COUNT_BOLT_ID, countBolt,3).fieldsGrouping(SPLIT_BOLT_ID,WORD_STREAM, new Fields("word")).allGrouping(SPLIT_BOLT_ID, END_OF_DATA_STREAM2);
		
		Config config = new Config();
		config.setNumWorkers(2);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.sleep(30000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

//	public static void main(String[] args) throws Exception {
//		SentenceSpout spout = new SentenceSpout();
//		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
//		WordCountBolt countBolt = new WordCountBolt();
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout(SENTENCE_SPOUT_ID, spout,2);
//		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID,SENTENCE_STREAM).allGrouping(SENTENCE_SPOUT_ID, END_OF_DATA_STREAM);
//		builder.setBolt(COUNT_BOLT_ID, countBolt,3).fieldsGrouping(SPLIT_BOLT_ID,WORD_STREAM, new Fields("word")).allGrouping(SPLIT_BOLT_ID, END_OF_DATA_STREAM2);
//		
//		Config config = new Config();
//		config.setNumWorkers(2);
//		LocalCluster cluster = new LocalCluster();
//		StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
//	}
	
}
