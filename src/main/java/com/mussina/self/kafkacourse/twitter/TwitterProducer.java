package com.mussina.self.kafkacourse.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("TwitterProducer run...");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        // create twitter client
        Client mussinaTwitterClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        mussinaTwitterClient.connect();

        // create kafka producer
        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!mussinaTwitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                mussinaTwitterClient.stop();
                logger.info("twitter client stop...");
            }
            if(msg != null)
                logger.info(msg);
        }
        logger.info("Application end...");
    }


    private Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(TwitterConstant.ConsumerKey, TwitterConstant.ConsumerSecret, TwitterConstant.Token, TwitterConstant.TokenSecret);

        ClientBuilder builder = new ClientBuilder()
            .name("MussinaTwitter-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        Client mussinaClient = builder.build();
        return mussinaClient;
    }
}
