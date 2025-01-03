package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

public class SentimentCountTopology {
    public static void main(String[] args) throws Exception {
//        buildWindowedTopology();
        buildSimpleTopology();
    }

    private static void buildSimpleTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout: Reads the CSV file
        builder.setSpout("csv-spout", new CSVSpout("twitter.csv"));

        // Bolt: Extracts game_name, sentiment, and adds processed_at timestamp
        builder.setBolt("filter-timestamp-bolt", new FilterAndTimestampBolt())
                .shuffleGrouping("csv-spout");

        // Bolt: Outputs results
        builder.setBolt("report-bolt", new ReportBolt())
                .shuffleGrouping("filter-timestamp-bolt");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1); // Number of worker processes
        config.setTopologyWorkerMaxHeapSize(15.0); // Max heap size in GB
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 15360); // Memory in MB
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 2.0); // CPU cores

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sentiment-count-topology", config, builder.createTopology());

        Utils.sleep(1000000);
        cluster.killTopology("sentiment-count-topology");
        cluster.shutdown();
    }

    private static void buildWindowedTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("csv-spout", new WindowCSVSpout("twitter.csv"));

        // Bolt: Extracts game_name, sentiment, and adds processed_at timestamp
        builder.setBolt("filter-timestamp-bolt", new WindowFilterAndTimestampBolt())
                .shuffleGrouping("csv-spout");

        // Define the bolt (processing unit)
        builder.setBolt("windowed-bolt", new WindowedAggregationBolt()
                        .withTumblingWindow(BaseWindowedBolt.Duration.minutes(5))
                        .withTimestampField("timestamp_mill"))
                .fieldsGrouping("filter-timestamp-bolt", new Fields("processed_at", "game_name", "sentiment"));

        // Bolt: Outputs results
        builder.setBolt("report-bolt", new WindowReportBolt())
                .shuffleGrouping("windowed-bolt");

        Config config = new Config();
        config.setDebug(true);
        config.setMessageTimeoutSecs(3600);

        // Running locally
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("windowed-topology", config, builder.createTopology());

        // Run for a while then shut down
        Utils.sleep(1000000);
        cluster.killTopology("sentiment-count-topology");
        cluster.shutdown();
    }
}

