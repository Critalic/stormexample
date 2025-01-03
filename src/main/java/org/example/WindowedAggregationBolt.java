package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class WindowedAggregationBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        Map<TupleMapKey, Long> counts = new HashMap<>();

        // Calculate window start and end time
        String windowStart = inputWindow.get().get(0).getStringByField("timestamp");
//        long windowEnd = inputWindow.get().get(inputWindow.get().size() - 1).getLongByField("timestamp");


        // Aggregate tuples in the current window
        for (Tuple input : inputWindow.get()) {
            String gameName = input.getStringByField("game_name");
            String sentiment = input.getStringByField("sentiment");
            String id = input.getStringByField("id");
            String timestamp = input.getStringByField("timestamp");
            String processedAt = input.getStringByField("processed_at");

            TupleMapKey key = new TupleMapKey(id, gameName, sentiment, processedAt, timestamp, windowStart, 0);

            counts.put(key, counts.getOrDefault(key, 0L) + 1);
        }

        // Emit aggregated results
        for (Map.Entry<TupleMapKey, Long> entry : counts.entrySet()) {
            TupleMapKey key = entry.getKey();
            collector.emit(new Values(key.getId(), key.getGameName(), key.getSentiment(), key.getProcessedAt(),
                    key.getTimestamp(), key.getWindowStart(), key.getWindowEnd(), entry.getValue()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "game_name", "sentiment", "processed_at", "timestamp", "window_start", "window_end", "count" ));
    }
}

class TupleMapKey {
    private final String id;
    private final String gameName;
    private final String sentiment;
    private final String processedAt;
    private final String timestamp;

    //TODO edit these to proper dates
    private final String windowStart;
    private final long windowEnd;

    public TupleMapKey(String id, String gameName, String sentiment, String processedAt, String timestamp, String windowStart, long windowEnd) {
        this.id = id;
        this.gameName = gameName;
        this.sentiment = sentiment;
        this.processedAt = processedAt;
        this.timestamp = timestamp;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleMapKey that = (TupleMapKey) o;
        return Objects.equals(gameName, that.gameName) && Objects.equals(sentiment, that.sentiment);
    }

    @Override
    public int hashCode() {
        return (gameName + sentiment).hashCode();
    }

    public String getId() {
        return id;
    }

    public String getGameName() {
        return gameName;
    }

    public String getSentiment() {
        return sentiment;
    }

    public String getProcessedAt() {
        return processedAt;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }
}
