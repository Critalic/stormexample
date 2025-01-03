package org.example;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

public class ReportBolt extends BaseBasicBolt {
    private BufferedWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            writer = new BufferedWriter(new FileWriter("results.csv", true)); // Append to file
        } catch (IOException e) {
            throw new RuntimeException("Error initializing file writer", e);
        }
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String gameName = tuple.getStringByField("game_name");
        String sentiment = tuple.getStringByField("sentiment");
        String id = tuple.getStringByField("id");
        String timestamp = tuple.getStringByField("timestamp");
//        int count = tuple.getIntegerByField("count");
        String processedAt = tuple.getStringByField("processed_at");

        String output = String.format("%s,%s,%s,%s,%s,%s%n",
                id, gameName, sentiment, timestamp, processedAt, Instant.now().toString());

//        String output = String.format("%s,%s,%s,%d,%s%n",
//                gameName, sentiment, processedAt, count, Instant.now().toString());
        try {
            writer.write(output);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file", e);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (writer != null) {
                writer.flush();
            }
        } catch (IOException e) {
            // Log error or handle cleanup failure
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields
    }
}