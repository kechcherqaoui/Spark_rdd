package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class FirstMain {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
              new SparkConf()
                    .setAppName("spark rdd tp")
                    .setMaster("local[*]")
        );
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> ventesRecords = sparkContext.textFile("ventes.txt");

        JavaRDD<String> villeRDD = ventesRecords.map(line -> line.split(" ")[1]);

        JavaPairRDD<String, Integer> villeCountPairs = villeRDD.mapToPair(ville -> new Tuple2<>(ville, 1));

        System.out.println("Total des ventes par ville.");
        for (Tuple2<String, Integer> villeCount : villeCountPairs.reduceByKey((a, b) -> a + b).collect())
            System.out.println(villeCount._1 + ": " + villeCount._2);

        sparkContext.stop();
    }
}