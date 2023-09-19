package com.shaunpark.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.net.URISyntaxException;
import java.time.Duration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;

public class App {
    Logger logger = LoggerFactory.getLogger(App.class);

    private String topic;
    private String fromConnector;
    private String toConnector;
    private String configFileName;
    private Properties props;

    private final static String PROP_OPTION = "p";
    private final static String COPY_OPTION = "c";
    private final static String LIST_OPTION = "l";
    private final static String GET_OPTION = "g";

    private final static String TO_CONNECTOR = "t";
    private final static String OFFSET_TOPIC_OPTION = "o";


    public static void main(String[] aa) throws Exception {

        Options options = new Options();        

        options.addOption(Option.builder(PROP_OPTION).required(true).hasArgs().longOpt("config-file").argName("CONFIG_FILE_NAME").desc("Config file to connect kafka broker.").build());
        // options.addOption(Option.builder().hasArgs().longOpt("from-connector").argName("FROM_CONNECTOR_NAME").build());
        options.addOption(Option.builder(TO_CONNECTOR)
                                .hasArgs()
                                .longOpt("to-connector")
                                .argName("TO_CONNECTOR_KEY")
                                .desc("Name of a destination connector")
                                .build());
        options.addOption(Option.builder(OFFSET_TOPIC_OPTION)
                                .required(true)
                                .hasArgs()
                                .longOpt("offset-topic")
                                .desc("Connector cluster offset topic name")
                                .argName("CONNECT_OFFSET_TOPIC").build());
        OptionGroup opOptions = new OptionGroup();

        opOptions.addOption(Option.builder(COPY_OPTION).longOpt("copy")
                    .hasArg()
                    .argName("FROM_CONNECTOR_KEY")
                    .desc("Copy offset of 'from connector' as a offset of 'to connector'.")
                    .build());
        opOptions.addOption(Option.builder(LIST_OPTION)
                    .longOpt("list")
                    .desc("List all connector name in the offset topic.")
                    .build());
        opOptions.addOption(Option.builder(GET_OPTION)
                    .longOpt("get")
                    .hasArg()
                    .argName("CONNECTOR_NAME")
                    .desc("Get a connector's offset from a connect offset topic with a connector name.")
                    .build());
        opOptions.setRequired(true);
        options.addOptionGroup(opOptions);

        CommandLineParser parser = new DefaultParser();

        App app = new App();
        try {
            CommandLine cmd = parser.parse(options, aa);

            app.configFileName = cmd.getOptionValue(PROP_OPTION);
            if( app.configFileName == null || app.configFileName.isBlank() ) {
                throw new MissingOptionException("Config filename isn't provieded.");
            }
            app.props = new Properties();
            app.props.load(new FileInputStream(app.configFileName));

            app.topic = cmd.getOptionValue(OFFSET_TOPIC_OPTION);
            if( app.topic == null || app.topic.isBlank() ) {
                throw new MissingOptionException("Connect offset topic name isn't provided.");
            }

            if (cmd.hasOption(GET_OPTION)){
                String connectorName = cmd.getOptionValue(GET_OPTION);
                if( connectorName != null && !connectorName.isBlank() ) {
                    app.fromConnector = connectorName;
                    app.getConnectorOffset(app.props);
                } else {
                    throw new MissingOptionException("From connector is needed to get connect offset info.");
                }
            } else if (cmd.hasOption(LIST_OPTION)){
                app.listConnectorNames(app.props);
            } else if (cmd.hasOption(COPY_OPTION)){
                app.fromConnector = cmd.getOptionValue(COPY_OPTION);
                app.toConnector = cmd.getOptionValue(TO_CONNECTOR);
                if( app.fromConnector == null || app.fromConnector.isBlank() ) {
                    throw new MissingOptionException("From Connector name isn't provided.");
                } 
                if( app.toConnector == null || app.toConnector.isBlank() ) {
                    throw new MissingOptionException("To Connector name isn't provided.");
                }
                app.copyConnectOffset(app.props);
            }

        } catch (MissingOptionException e) {
            app.printArgs(options, e.getMessage());
        }
    }

    private void getConnectorOffset(Properties props) {
        Hashtable<String, String> offsets = loadOffsets(props, getLasteOffsets(props, topic));
        Iterator<String> ii = offsets.keySet().iterator();
        boolean found = false;
        while(ii.hasNext()) {
            String key = ii.next();
            if( key.contains(fromConnector)){
                logger.info("Offset Key : " + key);
                logger.info("Offset Info : " + offsets.get(key) );
                found = true;
            }
        }
        if ( !found ) {
            System.out.println("Offset of connector " + fromConnector + " doesn't exist in offset topic" );
        }
    }

    private void copyConnectOffset(Properties props) throws Exception {
        Hashtable<String, String> offsets = loadOffsets(props, getLasteOffsets(props, topic));
        Iterator<String> ii = offsets.keySet().iterator();
        boolean findFrom = false;
        String value = "";

        while(ii.hasNext()) {
            String key = ii.next();
            if( key.equals(fromConnector)) {
                value = offsets.get(key);
                findFrom = true;
                break;
            }
        }

        if( !findFrom ) {
            throw new Exception("Connector offset of '" + fromConnector + "' doesn't exist in offset topic.");
        } else {
            try (Scanner scanner = new Scanner(System.in)) {
                System.out.println("Copy connector offset");
                System.out.println("From : " + fromConnector);
                System.out.println("To   : " + toConnector);
                System.out.println("Are you sure to copy the offset?[y/n]");

                String selection = scanner.nextLine();
                if( "y".equalsIgnoreCase(selection)) {
                    saveNewOffset(props, value);
                }
            }
        }
    }

    private void saveNewOffset(Properties props, String value) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, toConnector, value);
        try {
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    System.err.println("Failed to produce offset to the offset topic.");
                    e.printStackTrace();
                } else {
                    System.out.print("Offset is copied successfully.");
                }
            });
        } catch (Exception e) {
            System.err.println("Failed to produce offset to the offset topic.");
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close(Duration.ofSeconds(1));
        }
    }



    private void listConnectorNames(Properties props) {
        Hashtable<String, String> offsets = loadOffsets(props, getLasteOffsets(props, topic));

        Iterator<String> ii = offsets.keySet().iterator();
        System.out.println("Connectors in offset topic : ");
        while(ii.hasNext()) {
            String key = ii.next();
            System.out.println(" - " + key );
        }
    }

    private void printArgs(Options options, String message) throws URISyntaxException {
        String[] paths = App.class
        .getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .toURI()
        .getPath().split("/");
        String jarName = paths[paths.length - 1];
        if( !jarName.endsWith("jar")) {
            jarName = "app-0.0.1.jar";
        }

        System.out.println(message);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar " + jarName + " -p <CONFIG_FILE_NAME> -o <CONNECT_OFFSET_TOPIC> <Options>", options);
    }

    private Map<TopicPartition, Long> getLasteOffsets(Properties props, String topicName) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "srm-mig-load-key");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        final int numPartitions = consumer.partitionsFor(topicName).size();

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        logger.info("#### Number of Partitions : " + numPartitions);

        for( int i = 0 ; i < numPartitions; i++ ) {
            partitions.add(new TopicPartition(topicName, i));
        }
        Map<TopicPartition, Long> map = consumer.endOffsets(partitions, Duration.ofMinutes(1));

        consumer.close();

        return map;
    }


    public Hashtable<String, String> loadOffsets(Properties props, Map<TopicPartition, Long> offsetMap) {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Iterator<TopicPartition> iter = offsetMap.keySet().iterator();
        Hashtable<String, String> offsets = new Hashtable<>();

        while(iter.hasNext()) {
            TopicPartition tp = iter.next();
            long targetOffset = offsetMap.get(tp);
            if( targetOffset == 0 ) {
                continue;
            } else {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                List<TopicPartition> tpArray = Arrays.asList(tp);

                consumer.assign(tpArray);

                boolean keepGoing = true;
                consumer.seek(tp, 0);
                while(keepGoing) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if( records.count() == 0 ) {
                        keepGoing = false; // to exit the while loop
                        break; // to exit the for loop
                    }
                    for (ConsumerRecord<String, String> record : records){
                        if (record.offset() >= targetOffset){
                            keepGoing = false; // to exit the while loop
                            break; // to exit the for loop
                        } else {
                            offsets.put(record.key(), record.value());
                        }
                    }
                }
                consumer.close(Duration.ofMillis(100));
            }
        }  

        return offsets;
    }
}