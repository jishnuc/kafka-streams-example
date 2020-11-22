package com.github.jishnuc.kafka;

import com.github.jishnuc.kafka.admin.KafkaAdminController;
import com.github.jishnuc.kafka.consumer.BankBalanceConsumerApp;
import com.github.jishnuc.kafka.consumer.FavouriteColorConsumerApp;
import com.github.jishnuc.kafka.consumer.UserPurchaseConsumer;
import com.github.jishnuc.kafka.consumer.WordCountConsumerApp;
import com.github.jishnuc.kafka.producer.BankBalanceProducerApp;
import com.github.jishnuc.kafka.producer.FavouriteColorProducerApp;
import com.github.jishnuc.kafka.producer.UserDataProducer;
import com.github.jishnuc.kafka.producer.WordCountProducerApp;
import com.github.jishnuc.kafka.stream.BankBalanceStreamsApp;
import com.github.jishnuc.kafka.stream.FavouriteColorStreamsApp;
import com.github.jishnuc.kafka.stream.UserEventsEnricherApp;
import com.github.jishnuc.kafka.stream.WordCountStreamsApp;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
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
        logger.info("10. User Enricher Streams App");
        logger.info("11. User Data Producer App");
        logger.info("12. User Purchase Consumer App");
        logger.info("13. List Kafka Topics in Cluster");
        logger.info("14. List Kafka Nodes & Config in Cluster");
        logger.info("15. Create Kafka Topic");
        logger.info("16. Delete Kafka Topic");
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
                UserEventsEnricherApp ue=new UserEventsEnricherApp("user-table","user-purchases","user-purchases-enriched-inner-join","user-purchases-enriched-left-join");
                ue.run();
                break;
            case "11":
                UserDataProducer udp=new UserDataProducer();
                udp.run();
                break;
            case "12":
                UserPurchaseConsumer upc=new UserPurchaseConsumer("user-purchases-enriched-inner-join","user-purchases-enriched-left-join");
                upc.run();
                break;
            case "13":
                admin.listTopics();
                break;
            case "14":
                admin.printKafkaConfiguration();
                break;
            case "15":
                System.out.println("Enter Topic Name: ");
                String topicName = in.nextLine();
                System.out.println("Compacted topic (Y|N): ");
                String cleanupPolicy= in.nextLine();
                System.out.println("Enter Topic Partition: ");
                Integer partition = in.nextInt();
                System.out.println("Enter Topic Replication: ");
                Integer replication = in.nextInt();

                Map<String,String> config=new HashMap<>();
                if("Y".equals(cleanupPolicy) || "y".equals(cleanupPolicy)){
                    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
                }else{
                    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
                }
                admin.createTopic(topicName,partition,replication,config);
                break;
            case "16":
                System.out.println("Enter Topic Name: ");
                String deleteTopic = in.nextLine();
                admin.deleteTopic(deleteTopic);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
