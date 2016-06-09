package de.hu.bigdata.psd;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

public class Uebung3 {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Nutzung: Uebung3 <Pfad zur Datendatei> <min-supp>");
			System.exit(-1);
		}

		String ordersPfad = args[0];
		float minSupport = Float.parseFloat(args[1]);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//Basket Items: (Basket ID, Item ID)
		DataSet<Tuple2<Integer, Integer>> basketItems = env.readCsvFile(ordersPfad).fieldDelimiter("\t").types(Integer.class, Integer.class);
		//Baskets: (List<Item ID>, Size of Basket)
		DataSet<Tuple2<List<Integer>, Integer>> baskets = basketItems.groupBy(0).reduceGroup((values, out) -> {
			List<Integer> itemSet = new LinkedList<Integer>();
			int count = 0;
			for (Tuple2<Integer, Integer> tuple: values) {
				itemSet.add(tuple.f1);
				count++;
			}
			out.collect(new Tuple2<List<Integer>, Integer>(itemSet, count));
		});

		//Item count: (Item ID, Item Cound)
		DataSet<Tuple2<Integer, Integer>> itemCount = basketItems.map(tuple -> new Tuple2<Integer, Integer>(tuple.f1, 1)).
				groupBy(0).sum(1);
		// Total no. of baskets
		long basketCount = baskets.count();
		// Total no. of items
		long itemNo = itemCount.count();
		// Min Count
		double minCount = basketCount * minSupport;

		// Freq 1 Itemsets: (List<Item ID>, k)
		DataSet<Tuple2<List<Integer>, Integer>> freq1ItemSets = itemCount.filter(tuple -> tuple.f1 >= minCount).map(tuple -> {
			List<Integer> list = new LinkedList<Integer>();
			list.add(tuple.f0);
			return new Tuple2<List<Integer>, Integer>(list, 1);
		});

		DeltaIteration<Tuple2<List<Integer>, Integer>, Tuple2<List<Integer>, Integer>> iteration = freq1ItemSets.iterateDelta(freq1ItemSets, (int)itemNo, 0);

		// Candidate Set: (List<Item ID>, k)
		DataSet<Tuple2<List<Integer>, Integer>> candidateSet = aprioriGen(iteration.getWorkset());

		// reduced: (null, k)
		DataSet<Tuple2<List<Integer>, Integer>> reduced = candidateSet.reduce((value1, value2) -> new Tuple2<List<Integer>, Integer>(null, value1.f1));
		// Min support subsets: (List<Item ID>, k)
		DataSet<Tuple2<String, Integer>> minSupportSubsets = baskets.join(reduced).where(tuple -> true).equalTo(tuple -> true).with((tuple1, tuple2) -> new Tuple2<List<Integer>, Integer>(tuple1.f0, tuple2.f1)).flatMap((FlatMapFunction<Tuple2<List<Integer>, Integer>, Tuple2<String, Integer>>) (tuple, out) -> {
			int k = tuple.f1;
			List<Integer> list = new LinkedList<Integer>(tuple.f0);
			if (list.size() >= k) {
				for (int i = 0; i <= k - 2; i++) {
					list.remove(i);
					out.collect(new Tuple2<String, Integer>(list.toString(), 1));
				}
			}
		}).groupBy(0).sum(1).filter(tuple -> tuple.f1 >= minCount);

		// Delta (k frequent itemsets): (List<Item ID>, k)
		DataSet<Tuple2<List<Integer>, Integer>> delta = candidateSet.join(minSupportSubsets).where(tuple -> tuple.f0.toString()).equalTo(0).projectFirst(0);

		iteration.closeWith(delta, delta).print();

		// execute program
		env.execute("Team PSD Übung 3");
	}

	private static DataSet<Tuple2<List<Integer>, Integer>> aprioriGen(DataSet<Tuple2<List<Integer>, Integer>> freqKItemSets) {
		// Join Step
		DataSet<Tuple2<List<Integer>, Integer>> joinedSet = freqKItemSets.join(freqKItemSets).where((tuple) -> {
			List<Integer> list = new LinkedList<Integer>(tuple.f0);
			list.remove(list.size() - 1);
			return list.toString();
		}).equalTo((tuple) -> {
			List<Integer> list = new LinkedList<Integer>(tuple.f0);
			list.remove(list.size() - 1);
			return list.toString();
		}).filter(tuple -> tuple.f0.f0.get(tuple.f0.f0.size() - 1) < tuple.f1.f0.get(tuple.f1.f0.size() - 1)).map((tuple) -> {
			List<Integer> candidateList = tuple.f0.f0;
			candidateList.add(tuple.f1.f0.get(candidateList.size() - 1));
			return new Tuple2<List<Integer>, Integer>(candidateList, tuple.f1.f1 + 1);
		});
		// Prune Step
		//TODO: collect hier in join oder ähnliches umbauen
		DataSet<Tuple2<List<Integer>, Integer>> prunedSet = joinedSet.filter((tuple) -> {
			List<Tuple2<List<Integer>, Integer>> collectedItemSets = freqKItemSets.collect();
			List<Integer> list = new LinkedList<Integer>(tuple.f0);
			for (int i = 0; i <= tuple.f0.size() - 2; i++) {
				list.remove(i);
				for (Tuple2<List<Integer>, Integer> listTuple : collectedItemSets) {
					if (!list.toString().equals(listTuple.toString())) {
						return false;
					}
				}
			}
			return true;
		});
		return prunedSet;
	}
}
