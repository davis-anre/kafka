package org.isaacanteparac;

import com.fasterxml.uuid.Generators;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class kafka {

    public static void main(String[] args) {
        // Configuración de Kafka Streams
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", Config.IP.getString() + ":" + Config.PORT.getString());
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("acks", "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        final int theard = Integer.parseInt(Config.THEARDS.getString());
        ExecutorService executorService = Executors.newFixedThreadPool(theard);

        for (int i = 0; i < theard; i++) {
            executorService.submit(() -> {
                try {
                    startTopic(Topics.DURAN_PRODUCER, Topics.DURAN_CONSUMER, Regions.DURAN, producer);
                } catch (InterruptedException e) {
                    System.err.println("Error en DURAN: " + e.getMessage());
                }
            });

            executorService.submit(() -> {
                try {
                    startTopic(Topics.SAMBORONDON_PRODUCER, Topics.SAMBORONDON_CONSUMER, Regions.SAMBORONDON, producer);
                } catch (InterruptedException e) {
                    System.err.println("Error en DURAN: " + e.getMessage());
                }
            });

        }

        // Cierra el ExecutorService cuando termine
        executorService.shutdown();
    }

    public static void startTopic(Topics producerTopic, Topics consumerTopic, Regions region, KafkaProducer<String, String> producer) throws InterruptedException {
        GeneratorData generator = new GeneratorData();
        // Generar datos para el tópico de consumo
        generateDataForDuration(consumerTopic, region, generator, producer);

        // Configuración de Kafka Streams
        Properties streamsProps = new Properties();
        streamsProps.put("bootstrap.servers", Config.IP.getString() + ":" + Config.PORT.getString());
        streamsProps.put("application.id", "kafka-streams-app");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(producerTopic.getName());

        // Procesar los datos y enviarlos al tópico de consumo
        //source.mapValues(value -> value + " - procesado").to(consumerTopic.getName());

        // Iniciar Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        streams.start();

        // Shutdown hook para cerrar Kafka Streams correctamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Método para generar y enviar datos a un tópico Kafka
    public static void generateDataForDuration(
            Topics consumerTopic,
            Regions region,
            GeneratorData generator,
            KafkaProducer<String, String> producer
    ) throws InterruptedException {

        int seconds = Integer.parseInt(Config.DURATION_MINUTES.getString()) * 60;
        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.SECONDS.toMillis(seconds);

        while (System.currentTimeMillis() < endTime) {
            // Se genera id basado en tiempo, para que no haya repeticiones
            UUID uuid1 = Generators.timeBasedGenerator().generate();
            String id = uuid1.toString();
            try {
                String json = generator.generateElectricityData(id, region.getName());
                sendToInputTopic(producer, consumerTopic.getName(), id, json);
            } catch (Exception e) {
                System.err.println("Error al generar datos: " + e.getMessage());
            }
            Thread.sleep(Integer.parseInt(Config.LATENCY_MILLIS.getString()));
        }

        // Cierra el productor al final del proceso
        producer.close();
    }

    // Método para enviar mensajes a un tópico Kafka
    public static void sendToInputTopic(KafkaProducer<String, String> producer, String consumerTopic, String key, String value) {
        producer.send(new ProducerRecord<>(consumerTopic, key, value), (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error al enviar mensaje: " + exception.getMessage());
            } else {
                System.out.println("Mensaje enviado con éxito al tópico " + consumerTopic + " con offset " + metadata.offset());
            }
        });
    }



}
