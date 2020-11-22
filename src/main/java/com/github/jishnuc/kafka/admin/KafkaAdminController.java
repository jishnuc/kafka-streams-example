package com.github.jishnuc.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaAdminController {
    private static Logger    logger = LogManager.getLogger(KafkaAdminController.class);
    private final Properties config;

    public KafkaAdminController() {
        logger.info("Starting Kafka Admin Controller");
        config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    public void createTopic(String topic, int partition, int replicationFactor, Map<String, String> configs) {
        try (AdminClient admin = AdminClient.create(config)) {
            Set<String> names = admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get();

            if (!names.contains(topic)) {
                KafkaFuture<Config> topicStatus = admin.createTopics(Collections.singletonList(new NewTopic(topic,
                        partition,
                        (short) replicationFactor).configs(configs)))
                        .config(topic);


                topicStatus.get().entries().forEach(s -> logger.info(s.name() + "=" + s.value()));
            }
        } catch (Exception e) {
            logger.error("Error creating topics: ", e);
        }
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
            Collection<Node>              nodes      = nodeFuture.get();

            admin.describeConfigs(nodes.stream().map(n -> {
                    logger.info("-----Node: " + n);

                    return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(n.id()));
                })                     .collect(Collectors.toList())).all().whenComplete((configResourceConfigMap, th) -> {
                        if (th != null) {
                            logger.error("Unable to get broker config from server", th);
                        } else {
                            configResourceConfigMap.forEach(
                                (key, val) -> {
                                    logger.info(
                                    "=============================================================================");
                                    val.entries().forEach(c -> logger.info(c.name() + "=" + c.value()));
                                    logger.info(
                                    "=============================================================================");
                                });
                        }
                    });
        } catch (Exception e) {
            logger.error("Error Accessing Kafka Admin ", e);
        }
    }

    public void deleteTopic(String deleteTopic) {

        try(Admin admin=AdminClient.create(config)) {
            KafkaFuture<Void> topicStatus = admin.deleteTopics(Collections.singleton(deleteTopic)).all();
             topicStatus.get();
             logger.info(deleteTopic+" topic deleted successfully");

        } catch (InterruptedException e) {
            logger.error("Failed to Delete Topic "+deleteTopic,e);
        } catch (ExecutionException e) {
            logger.error("Failed to Delete Topic "+deleteTopic,e);

        }
    }
}


