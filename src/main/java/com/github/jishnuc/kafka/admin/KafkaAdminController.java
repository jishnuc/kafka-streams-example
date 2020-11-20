package com.github.jishnuc.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaAdminController {
    private static Logger    logger = LogManager.getLogger(KafkaAdminController.class);
    private final Properties config;

    public KafkaAdminController() {
        logger.info("Starting Kafka Admin Controller");
        config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    public void listTopics() {
        try (AdminClient admin = AdminClient.create(config)) {
            admin.listTopics().listings().whenComplete((topicListings, throwable) -> {
                    if (throwable != null) {
                        logger.error("Unable to get list of topics", throwable);
                    } else {
                        topicListings.forEach(logger::info);
                    }
                });
        } catch (Exception e) {
            logger.error("Error Accessing Kafka Admin ", e);
        }
    }

    public void printKafkaConfiguration() {
        try (AdminClient admin = AdminClient.create(config)) {

            KafkaFuture<Collection<Node>> nodeFuture = admin.describeCluster().nodes();

            Collection<Node> nodes = nodeFuture.get();

            admin.describeConfigs(nodes.stream().map(n -> {
                logger.info("-----Node: " + n);
                return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(n.id()));

            }).collect(Collectors.toList())).all().whenComplete((configResourceConfigMap, th) -> {
                if (th != null) {
                    logger.error("Unable to get broker config from server", th);
                } else {
                    configResourceConfigMap.forEach((key, val) -> {
                        logger.info("=============================================================================");
                        val.entries().forEach(c->logger.info(c.name()+"="+c.value()));
                        logger.info("=============================================================================");
                    });
                }
            });

        } catch (Exception e) {
            logger.error("Error Accessing Kafka Admin ", e);
        }
    }


    public void createTopic(String topic, int partition,int replicationFactor){
        try (AdminClient admin = AdminClient.create(config)) {
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false))
                    .names().get();
            if(!names.contains(topic)){
                admin.createTopics(Collections.singletonList(new NewTopic(topic,partition,(short)replicationFactor)))
                    .config(topic).whenComplete((c,e)->c.entries().forEach(s->logger.info(s.name()+"="+s.value())));
            }
        }catch (Exception e){
            logger.error("Error creating topics: ",e);
        }
    }
}


