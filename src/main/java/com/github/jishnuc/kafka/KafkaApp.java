package com.github.jishnuc.kafka;

import com.github.jishnuc.kafka.producer.FavouriteColorProducerApp;
import com.github.jishnuc.kafka.producer.WordCountProducerApp;
import com.github.jishnuc.kafka.stream.FavouriteColorStreamsApp;
import com.github.jishnuc.kafka.stream.WordCountStreamsApp;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaApp {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println(".............Starting the KafkaStreamsApp.....");
        System.out.println("Enter program you want to run");
        System.out.println("1. Word Count Streams App");
        System.out.println("2. Word Count Producer App");
        System.out.println("3. Word Count Consumer App");
        System.out.println("4. Favourite Color Streams App");
        System.out.println("5. Favourite Color Producer App");
        System.out.println("6. Favourite Color Consumer App");
        Scanner in= new Scanner(System.in);
        String choice = in.nextLine();
        switch(choice){
            case "1": WordCountStreamsApp wc=new WordCountStreamsApp("word-count-input","word-count-output");
                wc.run();
                break;
            case "2":
                WordCountProducerApp wcp=new WordCountProducerApp("word-count-input");
                wcp.run();
                break;
            case "3":break;
            case "4": FavouriteColorStreamsApp fc=new FavouriteColorStreamsApp("user-color-input","user-color","color-count-output") ;
                fc.run();
                break;
            case "5":
                FavouriteColorProducerApp fcp=new FavouriteColorProducerApp("user-color-input");
                fcp.run();
                break;
            case "6": break;
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }
}
