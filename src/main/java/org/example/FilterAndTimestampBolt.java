package org.example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.time.Instant;

public class FilterAndTimestampBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String gameName = input.getStringByField("game_name");
        String sentiment = input.getStringByField("sentiment");
        String id = input.getStringByField("id");
        String timestamp = input.getStringByField("timestamp");

        if (gameName != null && sentiment != null) {
            // Add processed_at timestamp
            String processedAt = Instant.now().toString();
            collector.emit(new Values(id, gameName, sentiment, processedAt, timestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "game_name", "sentiment", "processed_at", "timestamp"));
    }
}
