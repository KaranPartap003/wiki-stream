package com.wikistream;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;

@Service
public class KafkaProducer {

    @Autowired
    KafkaTemplate<String, String> message;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    public void sendMessage(){
        String Topic = "wikimedia-recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        //to read real time stream data from wikimedia, we use event source
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(message, Topic);
        URI uri = URI.create(url);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(ConnectStrategy.http(uri));
        BackgroundEventSource.Builder bgEventSourceBuilder = new BackgroundEventSource
                                                .Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource Source = bgEventSourceBuilder.threadPriority(Thread.MAX_PRIORITY).build();
        Source.start();
    }

}
