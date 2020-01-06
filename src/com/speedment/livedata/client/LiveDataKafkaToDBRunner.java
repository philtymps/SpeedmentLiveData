package com.speedment.livedata.client;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.InputStreamReader;
import java.lang.System;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LiveDataKafkaToDBRunner implements Runnable {

	private final	 AtomicBoolean closed = new AtomicBoolean(false);
    private 		 KafkaConsumer<String, String> consumer = null;
    
	@SuppressWarnings({ "deprecation", "unchecked" })
	@Override
	public void run()
	{
		Properties	kafkaProps = initializeKafkaProperties();
		consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new StringDeserializer());

		// TODO Auto-generated method stub
		try {
            consumer.subscribe(Arrays.asList("speedment-topic"));
            while (!closed.get())
            {
                ConsumerRecords<String, String> records = consumer.poll(10000);
                Iterator<?>		iRecords = records.iterator();
                long			iRecordCnt = 0;
                // Handle new records
                while (iRecords.hasNext())
                {
                	ConsumerRecord<String, String> record = (ConsumerRecord <String, String>)iRecords.next();
                    //System.out.println ("Key = " + (String)record.key() +  " Value = " + (String)record.value());
                	System.out.printf ("topic = %s, partition = %d, offset = %d,\"\r\n" + 
                			"                key = %s, value = %s\\n\",\r\n" + 
                			                record.topic(), record.partition(), record.offset(), 
                			                record.key(), record.value());
                    iRecordCnt++;
                }
                System.out.println ("Records Processed:" + iRecordCnt);
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
    
	public void waitForQuitKey() throws Exception
	{
	    System.out.println( "Q or q to end..." );
	    char answer = 'x';
	    InputStreamReader inputStreamReader = new InputStreamReader( System.in );
	    while( !( ( answer == 'q' ) || ( answer == 'Q' ) ) ) {
	      answer = (char) inputStreamReader.read();
	    }
	}

	protected Properties	initializeKafkaProperties ()
	{
		Properties	kafkaProps = new Properties();

		// since we're using transactional delivery we must set these properties
		kafkaProps.put("bootstrap.servers", "diab191.tympsnet.com:9092");
		kafkaProps.put("transactional.id",  "SPEEDMENT-RESET");
		kafkaProps.put("group.id", "SPEEDMENT");
		kafkaProps.put("auto.offset.reset", "earliest");
		kafkaProps.put("enable.auto.commit", "true");
		kafkaProps.put("auto.commit.interval.ms", "5000");
		
		//kafkaProps.put("isolation.level", "read_committed");
		// these properties below are if here if we decided against using transactional delivery
		/*
		 *
			kafkaProps.put("acks", "all");
			kafkaProps.put("retries", 0);
			kafkaProps.put("batch.size", 16384);
			kafkaProps.put("linger.ms", 1);
			kafkaProps.put("buffer.memory", 33554432);
			kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 */

		return kafkaProps;
	  }

}
