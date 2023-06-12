import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class MatrixMul implements Serializable{

    public static void main(String[] args) throws Exception {

        if (args.length < 6) {
            System.err.println("Usage: MatrixMul <matrix_a> <matrix_b> <m> <k> <n> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("MatrixMul")
            .getOrCreate();

        JavaRDD<String> mat1 = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> mat2 = spark.read().textFile(args[1]).javaRDD();

	int m = Integer.parseInt(args[2]);
	int k = Integer.parseInt(args[3]);
	int n = Integer.parseInt(args[4]);
	
        PairFlatMapFunction<String, String, Integer> pf1 = new PairFlatMapFunction<String, String, Integer>() {
            public Iterator<Tuple2<String, Integer>> call(String s) {
		ArrayList<Tuple2<String, Integer>> arr = new ArrayList<>();
		String[] tokens = s.split(" ");
                for (int i = 0; i < n; i++) {
			arr.add(new Tuple2<>(tokens[0] + " " + i + " " + tokens[1], Integer.parseInt(tokens[2])));
		}  
		return arr.iterator();
            }
        };
        JavaPairRDD<String, Integer> m1elements = mat1.flatMapToPair(pf1);	//PairRDD 만들기 
        
        PairFlatMapFunction<String, String, Integer> pf2 = new PairFlatMapFunction<String, String, Integer>() {
            public Iterator<Tuple2<String, Integer>> call(String s) {
		ArrayList<Tuple2<String, Integer>> arr = new ArrayList<>();
		String[] tokens = s.split(" ");
                for (int i = 0; i < m; i++) {
			arr.add(new Tuple2<>(i + " " +  tokens[1] + " " + tokens[0], Integer.parseInt(tokens[2])));
		}  
		return arr.iterator();
            }
        };
        JavaPairRDD<String, Integer> m2elements = mat2.flatMapToPair(pf2);	//PairRDD 만들기


	JavaPairRDD<String, Integer> elements = m1elements.union(m2elements); // get union

	Function2<Integer, Integer, Integer> f2mul = new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer x, Integer y) {
			return x * y;
		}
	};
	JavaPairRDD<String, Integer> mul = elements.reduceByKey(f2mul);
        
	JavaPairRDD<String, Integer> changeKey = mul.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
		public Tuple2<String, Integer> call(Tuple2<String, Integer> tp) {
			String key = tp._1; 
			String[] tokens = key.split(" ");
			return new Tuple2<>(tokens[0] + " " + tokens[1], tp._2);
		}
	});
	
	Function2<Integer, Integer, Integer> f2add = new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer x, Integer y) {
			return x + y;
		}
	};
	JavaPairRDD<String, Integer> rslt = changeKey.reduceByKey(f2add);
	rslt.saveAsTextFile(args[args.length - 1]);
        spark.stop();
    }
}

