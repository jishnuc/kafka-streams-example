package com.github.jishnuc.kafka;

import com.github.jishnuc.kafka.consumer.FavouriteColorConsumerApp;
import com.github.jishnuc.kafka.consumer.WordCountConsumerApp;
import com.github.jishnuc.kafka.producer.BankBalanceProducerApp;
import com.github.jishnuc.kafka.producer.FavouriteColorProducerApp;
import com.github.jishnuc.kafka.producer.WordCountProducerApp;
import com.github.jishnuc.kafka.stream.FavouriteColorStreamsApp;
import com.github.jishnuc.kafka.stream.WordCountStreamsApp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaApp {
    private static Logger logger = LogManager.getLogger(KafkaApp.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info(".............Starting the KafkaStreamsApp.....");
        logger.info("Enter program you want to run");
        logger.info("1. Word Count Streams App");
        logger.info("2. Word Count Producer App");
        logger.info("3. Word Count Consumer App");
        logger.info("4. Favourite Color Streams App");
        logger.info("5. Favourite Color Producer App");
        logger.info("6. Favourite Color Consumer App");
        logger.info("7. Bank Balance Streams App");
        logger.info("8. Bank Balance Producer App");
        logger.info("9. Bank Balance Consumer App");
        Scanner in= new Scanner(System.in);
        String choice = in.nextLine();
        switch(choice){
            case "1":
                WordCountStreamsApp wc=new WordCountStreamsApp("word-count-input","word-count-output");
                wc.run();
                break;
            case "2":
                WordCountProducerApp wcp=new WordCountProducerApp("word-count-input");
                wcp.run();
                break;
            case "3":
                WordCountConsumerApp wcc=new WordCountConsumerApp("word-count-output");
                wcc.run();
                break;
            case "4":
                FavouriteColorStreamsApp fc=new FavouriteColorStreamsApp("user-color-input","user-color","color-count-output") ;
                fc.run();
                break;
            case "5":
                FavouriteColorProducerApp fcp=new FavouriteColorProducerApp("user-color-input");
                fcp.run();
                break;
            case "6":
                FavouriteColorConsumerApp fcc=new FavouriteColorConsumerApp("color-count-output");
                fcc.run();
                break;
            case "7":
                break;
            case "8":
                BankBalanceProducerApp bap=new BankBalanceProducerApp("bank-balance-input");
                bap.run();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
