package de.hu.bigdata.psd;

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

		DataSet<Tuple2<Integer, Integer>> basketItems = env.readCsvFile(ordersPfad).fieldDelimiter("\t").types(Integer.class, Integer.class);
		DataSet<Tuple1<List<Integer>>> baskets = basketItems.groupBy(0).reduceGroup((values, out) -> {
			List<Integer> itemSet = new LinkedList<Integer>();
			for (Tuple2<Integer, Integer> tuple: values) {
				itemSet.add(tuple.f1);
			}
			out.collect(new Tuple1<List<Integer>>(itemSet));
		});

		DataSet<Tuple2<Integer, Integer>> itemCount = basketItems.groupBy(1).sum(1);
		long basketCount = baskets.count();
		long itemNo = itemCount.count();
		double minCount = basketCount * minSupport;

		DataSet<Tuple1<List<Integer>>> freq1ItemSets = itemCount.filter(tuple -> tuple.f1 >= minCount).map(tuple -> {
			List<Integer> list = new LinkedList<Integer>();
			list.add(tuple.f0);
			return new Tuple1<List<Integer>>(list);
		});

		DeltaIteration<Tuple1<List<Integer>>, Tuple1<List<Integer>>> iteration = freq1ItemSets.iterateDelta(freq1ItemSets, (int)itemNo, 0);

		DataSet<Tuple1<List<Integer>>> candidateSet = aprioriGen(iteration.getWorkset());

		DataSet<Tuple1<List<Integer>>> delta = candidateSet.filter((tuple) -> true);

		iteration.closeWith(delta, delta).print();

		// execute program
		env.execute("Team PSD Übung 3");
	}

	private static DataSet<Tuple1<List<Integer>>> aprioriGen(DataSet<Tuple1<List<Integer>>> freqKItemSets) {
		// Join Step
		DataSet<Tuple1<List<Integer>>> resultSet = freqKItemSets.join(freqKItemSets).where((tuple) -> {
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
			return new Tuple1<List<Integer>>(candidateList);
		});
		// Prune Step
		
		return resultSet;
	}

	/**
	 * Generischer Splitter, der ein Zweier-Tupel aus einem String erzeugt
	 * @param <T0>
	 *     		Erster Typ des Tupels
	 * @param <T1>
	 *     		Zweiter Typ des Tupels
     */
	private static class Tuple2Splitter<T0, T1> implements MapFunction<String, Tuple2<T0, T1>>, Serializable {

		private final int index1;
		private final int index2;
		private final String delimiter;
		private final Function<String, T0> t0Parser;
		private final Function<String, T1> t1Parser;

		/**
		 *
		 * @param delimiter
		 * 			Trennzeichen
		 * @param index1
		 * 			Index, an dem sich das erste Element des Tupels befindet
		 * @param index2
		 * 			Index, an dem sich das zweite Element des Tupels befindet
		 * @param t0Parser
		 * 			Funktion zum Parsen des ersten Elements
         * @param t1Parser
		 * 			Funktion zum Parsen des zweiten Elements
         */
		Tuple2Splitter(String delimiter, int index1, int index2, Function<String, T0> t0Parser, Function<String, T1> t1Parser) {
			this.index1 = index1;
			this.index2 = index2;
			this.delimiter = delimiter;
			this.t0Parser = t0Parser;
			this.t1Parser = t1Parser;
		}

		@Override
		public Tuple2<T0, T1> map(String s) throws Exception {
			String[] array = s.split(delimiter);
			return new Tuple2<>(t0Parser.apply(array[index1]), t1Parser.apply(array[index2]));
		}
	}

	/**
	 * Generischer Splitter, der ein Dreier-Tupel aus einem String erzeugt
	 * @param <T0>
	 *     		Erster Typ des Tupels
	 * @param <T1>
	 *     		Zweiter Typ des Tupels
	 * @param <T2>
	 *     		Dritter Typ des Tupels
	 */
	private static class Tuple3Splitter<T0, T1, T2> implements MapFunction<String, Tuple3<T0, T1, T2>>, Serializable {

		private final int index1;
		private final int index2;
		private final int index3;
		private final String delimiter;
		private final Function<String, T0> t0Parser;
		private final Function<String, T1> t1Parser;
		private final Function<String, T2> t2Parser;

		/**
		 *
		 * @param delimiter
		 * 			Trennzeichen
		 * @param index1
		 * 			Index, an dem sich das erste Element des Tupels befindet
		 * @param index2
		 * 			Index, an dem sich das zweite Element des Tupels befindet
		 * @param index3
		 * 			Index, an dem sich das dritte Element des Tupels befindet
		 * @param t0Parser
		 * 			Funktion zum Parsen des ersten Elements
		 * @param t1Parser
		 * 			Funktion zum Parsen des zweiten Elements
		 * @param t2Parser
		 * 			Funktion zum Parsen des dritten Elements
		 */
		Tuple3Splitter(String delimiter, int index1, int index2, int index3, Function<String, T0> t0Parser, Function<String, T1> t1Parser, Function<String, T2> t2Parser) {
			this.index1 = index1;
			this.index2 = index2;
			this.index3 = index3;
			this.delimiter = delimiter;
			this.t0Parser = t0Parser;
			this.t1Parser = t1Parser;
			this.t2Parser = t2Parser;
		}

		@Override
		public Tuple3<T0, T1, T2> map(String s) throws Exception {
			String[] array = s.split(delimiter);
			return new Tuple3<>(t0Parser.apply(array[index1]), t1Parser.apply(array[index2]), t2Parser.apply(array[index3]));
		}
	}
}
