import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class Join implements Serializable{

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Join <in-file> <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("Join")
            .getOrCreate();

        JavaRDD<String> products = spark.read().textFile(args[0]).javaRDD(); //한줄 한줄이 large array가 된다.

        PairFunction<String, String, Product> pfA = new PairFunction<String, String, Product>() {
            public Tuple2<String, Product> call(String s) {
		String[] tokens = s.split("\\|");
                return new Tuple2(tokens[2], new Product(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]), tokens[2]));
            }
        };
        JavaPairRDD<String, Product> pTuples = products.mapToPair(pfA);	//PairRDD 만들기 : 단어와 1을 pair로 저장! 이때 사용하는 함수가 pf => 단어 하나를 받아서 (단어, 1)인 튜플을 반환

	JavaRDD<String> codes = spark.read().textFile(args[1]).javaRDD();

	PairFunction<String, String, Code> pfB = new PairFunction<String, String, Code>() {
		public Tuple2<String, Code> call(String s) {
			String[] tokens = s.split("\\|");
			return new Tuple2(tokens[0], new Code(tokens[0], tokens[1]));
		}
	};
	JavaPairRDD<String, Code> cTuples = codes.mapToPair(pfB);

	JavaPairRDD<String, Tuple2<Product, Code>> joined = pTuples.join(cTuples); // inner-join

        joined.saveAsTextFile(args[2]);
        spark.stop();
    }
}

class Product implements Serializable {
	int id;
	int price;
	String code;

	public Product() {};
	public Product(int _id, int _price, String _code) {
		id = _id;
		price = _price;
		code = _code;
	}

	public String toString() {
		return ("id : " + id  + ", price : " + price + ", code : " + code);
	}
}

class Code implements Serializable {
	String code;
	String description;

	public Code() {};
	public Code(String _code, String _description) {
		code = _code;
		description = _description;
	}
	
	public String toString() {
		return ("code : " + code + ", desc: " + description);
	}
	
}
