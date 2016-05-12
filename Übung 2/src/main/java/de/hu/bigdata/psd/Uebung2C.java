package de.hu.bigdata.psd;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

public class Uebung2C {

	private static DateFormat dateFormat = new DateFormat();

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Nutzung: Uebung2C <Pfad zur Orders.tbl> <Pfad zur Lineitems.tbl> <Orderstatus-Filter> <Shipdate-Filter Von im Format \"YYYY-MM-DD\"> <Shipdate-Filter Bis im Format \"YYYY-MM-DD\">");
			System.exit(-1);
		}

		String ordersPfad = args[0];
		String lineitemsPfad = args[1];
		Character orderStatusFilter = args[2].charAt(0);
		String shipDateFromFilterString = args[3];
		String shipDateToFilterString = args[4];
		Date shipDateToFilter = null;
		Date shipDateFromFilter = null;
		try {
			shipDateToFilter = dateFormat.parse(shipDateToFilterString);
			shipDateFromFilter = dateFormat.parse(shipDateFromFilterString);
		} catch (ParseException e) {
			System.err.println("Nutzung: Uebung2C <Pfad zur Orders.tbl> <Pfad zur Lineitems.tbl> <Orderstatus-Filter> <Shipdate-Filter Von im Format \"YYYY-MM-DD\"> <Shipdate-Filter Bis im Format \"YYYY-MM-DD\">");
			System.exit(-1);
		}
		final Date finalShipDateToFilter = shipDateToFilter;
		final Date finalShipDateFromFilter = shipDateFromFilter;

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> summedOrders = sumOrders(ordersPfad, lineitemsPfad, orderStatusFilter, finalShipDateToFilter, finalShipDateFromFilter, env);

		summedOrders.print();

		// execute program
		env.execute("Team PSD Übung 2c");
	}

	/**
	 * Eigentlicher Ausführungsgraph
     */
	private static DataSet<Tuple2<Long, Double>> sumOrders(String ordersPfad, String lineitemsPfad, Character orderStatusFilter, Date finalShipDateToFilter, Date finalShipDateFromFilter, ExecutionEnvironment env) {
		DataSet<Tuple2<Long, Character>> orders = env.readTextFile(ordersPfad).map(new OrderSplitter());
		DataSet<Tuple3<Long, Double, Date>> lineItems = env.readTextFile(lineitemsPfad).map(new LineItemsSplitter());
		DataSet<Tuple2<Long, Character>> filteredOrders = orders.filter(tuple -> tuple.f1.equals(orderStatusFilter));
		DataSet<Tuple3<Long, Double, Date>> filteredLineItems = lineItems.filter(tuple -> tuple.f2.before(finalShipDateToFilter) && !tuple.f2.before(finalShipDateFromFilter));
		DataSet<Tuple2<Long, Double>> joinedOrders = filteredOrders.joinWithHuge(filteredLineItems).where(0).equalTo(0).with((tuple1, tuple2) -> new Tuple2<Long, Double>(tuple1.f0, tuple2.f1));
		return joinedOrders.groupBy(0).aggregate(Aggregations.SUM, 1);
	}

	private static final class DateFormat extends SimpleDateFormat {
		DateFormat() {
			super("yyyy-MM-dd");
		}
	}

	/**
	 * Splitter für die Order-Tabelle
	 */
	private static final class OrderSplitter extends Tuple2Splitter<Long, Character> implements Serializable {
		private static final int ORDER_KEY_POSITION = 0;
		private static final int ORDER_STATUS_POSITION = 2;

		OrderSplitter() {
			super("\\|", ORDER_KEY_POSITION, ORDER_STATUS_POSITION, (Function<String, Long> & Serializable) Long::parseLong, (Function<String, Character> & Serializable) value -> value.charAt(0));
		}
	}

	/**
	 * Splitter für die Lineitems-Tabelle
	 */
	private static final class LineItemsSplitter extends Tuple3Splitter<Long, Double, Date> implements Serializable {
		private static final int ORDER_KEY_POSITION = 0;
		private static final int EXTENDED_PRICE_POSITION = 5;
		private static final int SHIPDATE_POSITION = 10;

		LineItemsSplitter() {
			super("\\|", ORDER_KEY_POSITION, EXTENDED_PRICE_POSITION, SHIPDATE_POSITION, (Function<String, Long> & Serializable) Long::parseLong, (Function<String, Double> & Serializable) Double::parseDouble, (Function<String, Date> & Serializable)(value) -> {
				SimpleDateFormat lineItemsDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				try {
					return lineItemsDateFormat.parse(value);
				} catch (ParseException e) {
					return new Date();
				}
			});
		}

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
