import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("JavaWordCount")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD(); //한줄 한줄이 large array가 된다.

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() { //줄단위를 날리기 위해 FlatMapFunction 적용
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        };
        JavaRDD<String> words = lines.flatMap(fmf); //flatMap 메소드에 적용하면 한줄 한줄이 아니라 단어 기준으로 array가 만들어짐

        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s, 1);
            }
        };
        JavaPairRDD<String, Integer> ones = words.mapToPair(pf);	//PairRDD 만들기 : 단어와 1을 pair로 저장! 이때 사용하는 함수가 pf => 단어 하나를 받아서 (단어, 1)인 튜플을 반환

        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        };
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2); // 같은 단어별로 value를 reduce. 이때 사용하는 함수는 f2 => Function2타입으로, integer 2개를 더해줌. 

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
