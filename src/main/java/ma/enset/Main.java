package ma.enset;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "text-analysis-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream("text-input");

        KStream<String, String> validStream = sourceStream
                .filter((key, value) -> value != null)
                .mapValues(value -> value.trim().replaceAll("\\s+", " ").toUpperCase())
                .filter((key, value) ->
                        !value.isBlank()
                                && value.length() <= 100
                                && !value.contains("HACK")
                                && !value.contains("SPAM")
                                && !value.contains("XXX")
                );

        KStream<String, String> invalidStream = sourceStream.filter((key, value) -> {
            if (value == null) return true;

            String cleaned = value.trim().replaceAll("\\s+", " ").toUpperCase();

            return cleaned.isBlank()
                    || cleaned.length() > 100
                    || cleaned.contains("HACK")
                    || cleaned.contains("SPAM")
                    || cleaned.contains("XXX");
        });

        validStream.to("text-clean");
        invalidStream.to("text-dead-letter");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Text analysis Kafka Streams started...");
    }
}