import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.Iterator;

public class UBERStudent20200941 {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20200941 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20200941")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();  	//한줄 한줄이 large array가 된다.

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {  	//줄단위를 날리기 위해 FlatMapFunction 적용
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s).iterator();
            }
        });

        // 지역과 요일별로 trips와 vehicles를 계산한다: Tuple2 활용하여 key도 2개, value도 2개씩..
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> ones = words.mapToPair(new PairFunction<String, Tuple2<String, String>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>> call(String s) {
                String[] tokens = s.split(",");
                String region = tokens[0];
                String[] dates = tokens[1].split("/");
                int trips = Integer.parseInt(tokens[3]);
                int vehicles = Integer.parseInt(tokens[2]);

                int m = Integer.parseInt(dates[0]);
                int d = Integer.parseInt(dates[1]);
                int y = Integer.parseInt(dates[2]);
                java.time.LocalDate localDate = java.time.LocalDate.of(y, m, d);
                java.time.DayOfWeek dayOfWeek = localDate.getDayOfWeek();
                
                String day = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
                if (day.equals("THU")) day = "THR";

                Tuple2<String, String> key = new Tuple2<>(region, day);
                Tuple2<Integer, Integer> value = new Tuple2<>(trips, vehicles);

                return new Tuple2<>(key, value);
            }
        });

        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> counts = ones.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) {
                int tripsSum = value1._1() + value2._1();
                int vehiclesSum = value1._2() + value2._2();
                return new Tuple2<>(tripsSum, vehiclesSum);
            }
        });

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
