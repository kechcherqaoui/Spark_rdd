package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Scanner;

public class SecondMain {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
              new SparkConf()
                    .setAppName("spark rdd tp")
                    .setMaster("local[*]")
        );

        sparkContext.setLogLevel("ERROR");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Please enter the year: ");

        int year = scanner.nextInt();

        JavaRDD<String> ventesRecords = sparkContext.textFile("ventes.txt");

        JavaRDD<String> filteredVentesRecords = ventesRecords.filter(s -> {
            int ventYear = Integer.parseInt(s.split(" ")[0].split("-")[2]);
            return ventYear == year;
        });

        JavaPairRDD<String, Double> venteDataRDD = filteredVentesRecords.mapToPair(line -> {
            String[] venteData = line.split(" ");
            String city = venteData[1];
            double price = Double.parseDouble(venteData[3]);

            return new Tuple2<>(city, price);
        });

        System.out.println("Le prix total des ventes des produits par ville pour l'année " + year );
        for (Tuple2<String, Double> cityPrice : venteDataRDD.reduceByKey(Double::sum).collect())
            System.out.println(cityPrice._1 + " " + cityPrice._2);

        sparkContext.stop();
    }
}