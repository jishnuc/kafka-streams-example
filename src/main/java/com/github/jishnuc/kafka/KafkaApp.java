package com.github.jishnuc.kafka;

import com.github.jishnuc.kafka.admin.KafkaAdminController;
import com.github.jishnuc.kafka.consumer.BankBalanceConsumerApp;
import com.github.jishnuc.kafka.consumer.FavouriteColorConsumerApp;
import com.github.jishnuc.kafka.consumer.WordCountConsumerApp;
import com.github.jishnuc.kafka.producer.BankBalanceProducerApp;
import com.github.jishnuc.kafka.producer.FavouriteColorProducerApp;
import com.github.jishnuc.kafka.producer.WordCountProducerApp;
import com.github.jishnuc.kafka.stream.BankBalanceStreamsApp;
import com.github.jishnuc.kafka.stream.FavouriteColorStreamsApp;
import com.github.jishnuc.kafka.stream.WordCountStreamsApp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaApp {
    private static Logger logger = LogManager.getLogger(KafkaApp.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaAdminController admin=new KafkaAdminController();
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
        logger.info("10. List Kafka Topics in Cluster");
        logger.info("11. List Kafka Nodes & Config in Cluster");
        logger.info("12. Create Kafka Topic");
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
                BankBalanceStreamsApp bb=new BankBalanceStreamsApp("bank-balance-input","bank-balance-output");
                bb.run();
                break;
            case "8":
                BankBalanceProducerApp bbp=new BankBalanceProducerApp("bank-balance-input");
                bbp.run();
                break;
            case "9":
                BankBalanceConsumerApp bbc=new BankBalanceConsumerApp("bank-balance-output");
                bbc.run();
                break;
            case "10":
                admin.listTopics();
                break;
            case "11":
                admin.printKafkaConfiguration();
                break;
            case "12":
                System.out.println("Enter Topic Name: ");
                String topicName = in.nextLine();
                System.out.println("Enter Topic Partition: ");
                Integer partition = in.nextInt();
                System.out.println("Enter Topic Replication: ");
                Integer replication = in.nextInt();
                admin.createTopic(topicName,partition,replication);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
