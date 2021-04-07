package flink01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import utils.FlatMapFunc;

public class WordCountBatch {


    public static void main(String[] args) throws Exception {

        LocalEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

        DataSource<String> stringDataSource = environment.readTextFile("src/main/resources/word.txt");

        FlatMapOperator<String, Tuple2<String,Integer>> flatMap = stringDataSource.flatMap(new FlatMapFunc());

        UnsortedGrouping<Tuple2<String,Integer>> tuple2UnsortedGrouping = flatMap.groupBy(0);

        AggregateOperator<Tuple2<String,Integer>> sum = tuple2UnsortedGrouping.sum(1);

        sum.print();

    }
}
