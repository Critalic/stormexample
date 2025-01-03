package org.example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {
    private Map<String, Map<String, Integer>> sentimentCounts = new HashMap<>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String gameName = input.getStringByField("game_name");
        String sentiment = input.getStringByField("sentiment");
        String processedAt = input.getStringByField("processed_at");

        sentimentCounts.putIfAbsent(gameName, new HashMap<>());
        Map<String, Integer> gameCounts = sentimentCounts.get(gameName);
        gameCounts.put(sentiment, gameCounts.getOrDefault(sentiment, 0) + 1);

        collector.emit(new Values(gameName, sentiment, gameCounts.get(sentiment), processedAt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_name", "sentiment", "count", "processed_at"));
    }
}
