package org.example;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import  java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //который дает уникальный идентификатор вашего приложения Streams, чтобы отличить его от других. приложения, взаимодействующие с одним и тем же кластером Kafka
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //определяет список пар хост/порт, которые будут использоваться для установления первоначального соединения с кластером Kafka,
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //библиотеки сериализации и десериализации по умолчанию для пар ключ-значение записи
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Kafka Streams эта вычислительная логика определяется как topologyобъединение узлов процессора. Мы можем использовать построитель топологии для построения такой топологии:
        final StreamsBuilder builder = new StreamsBuilder();

        // затем создайте исходный поток из темы Kafka с именем, streams-plaintext-inputиспользуя этот построитель топологии:
        KStream<String, String> source = builder.stream("streams-plaintext-input");

        //Теперь мы получаем объект KStream, который постоянно генерирует записи из исходной темы Kafka streams-plaintext-input. Записи организованы в виде Stringтипизированных пар ключ-значение. Самое простое, что мы можем сделать с этим потоком, — это записать его в другую тему Kafka, скажем, с именем streams-pipe-output:
        source.to("streams-pipe-output");

        //Мы можем проверить, какой тип topologyсоздан с помощью этого конструктора, выполнив следующие действия:
        final Topology topology = builder.build();

        //И выведите его описание в стандартный вывод как:
        System.out.println(topology.describe());
        //Предположим, мы уже закончили с этой простой топологией, которая просто передает данные из одной темы Kafka в другую в бесконечном потоковом режиме. Теперь мы можем создать клиент Streams с двумя компонентами, которые мы только что создали выше: карту конфигурации, указанную в экземпляре, java.util.Propertiesи объект Topology.
        final KafkaStreams streams = new KafkaStreams(topology, props);

        //ызвав его start()функцию, мы можем инициировать выполнение этого клиента. Выполнение не остановится, пока close()оно не будет вызвано на этом клиенте. Мы можем, например, добавить перехватчик завершения работы с защелкой обратного отсчета, чтобы перехватывать пользовательское прерывание и закрывать клиент после завершения этой программы:
        final CountDownLatch latch = new CountDownLatch(1);

// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
