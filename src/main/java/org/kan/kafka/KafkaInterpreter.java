package org.kan.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.zeppelin.interpreter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kevin on 9/3/16.
 */
public class KafkaInterpreter extends Interpreter{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaInterpreter.class);
    private KafkaConsumer consumer;

    public KafkaInterpreter(Properties props) {
        super(props);
    }

    @Override
    public void open() {
        logger.info("Opening Kafka Consumer");
        Properties consumerProps = new Properties();
        //consumerProps.setProperty();
        consumer = new KafkaConsumer(consumerProps);
    }

    @Override
    public void close() {
        LOGGER.info("Closing Kafka Consumer");
        consumer.close();
    }

    @Override
    public InterpreterResult interpret(String topic, InterpreterContext interpreterContext) {
        //Check for null
        if (topic == null || topic.trim().length() == 0) {
            return new InterpreterResult(InterpreterResult.Code.SUCCESS);
        }

        logger.info("Subscribe to topic: " + topic);
        consumer.subscribe(Arrays.asList("topic"));
        InterpreterResult result = new InterpreterResult(InterpreterResult.Code.SUCCESS, consumer.poll(0).toString());
        //while(true) {
            //consumer.poll(0);

        //}


        return result;
    }

    @Override
    public void cancel(InterpreterContext interpreterContext) {
        LOGGER.info("Canceling Kafka Consumer");
        consumer.close();

    }

    @Override
    public FormType getFormType() {
        return FormType.NONE;
    }

    @Override
    public int getProgress(InterpreterContext interpreterContext) {
        return 0;
    }
}
