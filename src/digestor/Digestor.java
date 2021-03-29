package digestor;

import static java.lang.Math.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/***
 * Utilidad para agrupar y agregar un flujo o una tabla  de filas de datos numericos
 * El procesamiento puede ser serial o parallelo 
 * Consume filas de tamanio homogeneo en forma de Number[], donde los elementos de la filas son los valores de las columas 
 * Una fila vacia sirve de terminal EOF del flujo
 * Por el momento las agregaciones soportadas son COUNT, MIN, MAX, SUM, AVG, STDEV
 *  
 * @author Ivan Zuniga Urena
 *
 */

public class Digestor {
	enum Opcode {
		COUNT, MIN, MAX, SUM, AVG, STDEV
	};
	/*
	 * Retorna un Collector que realiza las agregaciones especificadas sobre la agrupacion de una columna
	 *    
	 */
	public Collector getGroupingBy(int i, Opcode[] ops) {
		Collector g = Collectors.groupingBy(tuple -> tuple[i], new Digestor.ArrayCollector(ops));
		return g;
	}

	public Collector<Number[], ?, Map<Number, List<Number[]>>> getGroupingBy(int i) {
		Collector<Number[], ?, Map<Number, List<Number[]>>> g;
		g = Collectors.groupingBy(tuple -> tuple[i]);
		return g;
	}

	public Collector getGroupingBy(int i, Collector<Number[], ?, Map<Number, List<Number[]>>> ng) {
		Collector<Number[], ?, Map<Object, Map<Number, List<Number[]>>>> g;
		g = Collectors.groupingBy(tuple -> tuple[i], ng);
		return g;
	}

	public static Stream<Number[]> getContinuosStream(BlockingQueue<Number[]> queue) {
		Spliterator<Number[]> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.IMMUTABLE) {
			boolean done;
			@Override
			public boolean tryAdvance(Consumer<? super Number[]> action) {
				Number[] e = null;
				try {
					if(queue.isEmpty() && done){
						return false;
					}
					e = queue.take();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if (e.length != 0) {
					action.accept(e);
					return true;
				}
				done = true;
				return false;
			}
		};
		return StreamSupport.stream(spliterator, false);
	}
	
	public static Stream<Number[]> getInfiniteStream(BlockingQueue<Number[]> queue) {
		return Stream.generate((Supplier<? extends Number[]>) () -> {
			Number[] e = null;
			try {
				e = queue.take();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return e;
		});
	}
	
	class ArrayCollector implements Collector<Number[], ArrayAcummulator, Number[]> {
		Opcode[] opcode;

		ArrayCollector(Opcode[] opcode) {
			this.opcode = opcode;
		}

		@Override
		public Supplier<ArrayAcummulator> supplier() {
			return () -> new ArrayAcummulator(opcode);
		}

		@Override
		public BiConsumer<ArrayAcummulator, Number[]> accumulator() {
			return ArrayAcummulator::accumulator;
		}

		@Override
		public BinaryOperator<ArrayAcummulator> combiner() {
			return ArrayAcummulator::combiner;
		}

		@Override
		public Function<ArrayAcummulator, Number[]> finisher() {
			return ArrayAcummulator::finisher;
		}

		@Override
		public Set<Characteristics> characteristics() {
			Set<Characteristics> chars = new HashSet<Collector.Characteristics>();
			chars.add(Characteristics.CONCURRENT);
			return chars;
		}
	}

	class ArrayAcummulator {
		int size;
		Opcode[] op;
		Aggregator[] aggregator;
		Number[] c;

		public ArrayAcummulator(Opcode[] opcode) {
			op = opcode;
			size = op.length;
			c = new Number[size];
			Arrays.fill(c, 0);
			for (int i = 0; i < op.length; i++) {
				switch(op[i]){
				case MIN:
					c[i] =	Double.MAX_VALUE;
					break;
				case AVG:
					c[i] =	new Avg();
					break;
				case STDEV:
					c[i] =	new StDev();
					break;
				default:
				}
			}

			aggregator = new Aggregator[size];

			for (int i = 0; i < op.length; i++) {
				Aggregator a = new Aggregator(op[i]);
				aggregator[i] = a;
			}
		}

		public void accumulator(Number[] a) {
			for (int i = 0; i < a.length; i++) {
				c[i] = aggregator[i].accumulator.apply(c[i], a[i]);
			}
		}

		public ArrayAcummulator combiner(ArrayAcummulator other) {
			this.finisher();
			other.finisher();
			for (int i = 0; i < this.c.length; i++) {
				c[i] = aggregator[i].combiner.apply(this.c[i], other.c[i]);
			}			
			return this;
		}

		public Number[] finisher() {
			for (int i = 0; i < c.length; i++) {
				c[i] = aggregator[i].finisher.apply(c[i]);
			}
			return c;
		}

		class Aggregator {
			BinaryOperator<Number> accumulator;
			BinaryOperator<Number> combiner;
			Function<Number, Number> finisher;

			public Aggregator(Opcode op) {
				accumulator = accumulator(op);
				combiner = combiner(op);
				finisher = finisher(op);
			}

			BinaryOperator<Number> accumulator(Opcode op) {
				switch (op) {
				case COUNT:
					return (a, b) -> {
						return addNumbers(a, 1);
					};
				case MIN:
					return (a, b) -> {
						return a.intValue() < b.intValue() ? a : b;
					};
				case MAX:
					return (a, b) -> {
						return a.intValue() > b.intValue() ? a : b;
					};
				case SUM:
					return (a, b) -> {
						return addNumbers(a, b);
					};
				case AVG:
					return (a, b) -> {
						((Avg)a).put(b);
						return a;
					};
				case STDEV:
					return (a, b) -> {
						((StDev)a).put(b);
						return a;
					};
				}
				
				return null;
			}

			BinaryOperator<Number> combiner(Opcode op) {
				switch (op) {
				case COUNT:
					return (a, b) -> {
						return addNumbers(a, b);
					};
				case MIN:
					return (a, b) -> {
						return a.intValue() < b.intValue() ? a : b;
					};
				case MAX:
					return (a, b) -> {
						return a.intValue() > b.intValue() ? a : b;
					};
				case SUM:
					return (a, b) -> {
						return addNumbers(a, b);
					};
				case AVG:
					return (a, b) -> {
						return addNumbers(a, b).doubleValue() / 2;
					}; // fix per type loose precision
				case STDEV:
					return (a, b) -> {
						((StDev)a).combine((StDev)b);
						return a;
					};
				}
				return null;
			};

			Function<Number, Number> finisher(Opcode op) {
				switch (op) {
				case MIN:
				case MAX:
				case SUM:
				case COUNT:
					return (a) ->  a;
				case AVG:
					return (a) -> ((Avg)a).get();
				case STDEV:
					return (a) -> ((StDev)a).get();
				}
				return null;
			};
		}
	}
	
	static void print(Map<?, ?> group) {
		printrec(group, "");
	}

	static void printrec(Map<?, ?> group, String level) {
		group.forEach((i, e) -> {

			if (e instanceof Map) {
				printrec((Map<?, ?>) e, level + i.toString() + " ");
			} else if (e instanceof Number[]) {
				System.out.print(level + i.toString() + " \t");
				System.out.print(Arrays.toString((Number[]) e));
				System.out.println();
			} else {
				System.out.print(level + i.toString() + " \t");
				((Iterable<Number[]>) e).forEach(x -> System.out.print(Arrays.toString(x)));
				System.out.println();
			}
		});
	}

	public static Number addNumbers(Number a, Number b) {
		if (a instanceof Double || b instanceof Double) {
			return a.doubleValue() + b.doubleValue();
		} else if (a instanceof Float || b instanceof Float) {
			return a.floatValue() + b.floatValue();
		} else if (a instanceof Long || b instanceof Long) {
			return a.longValue() + b.longValue();
		} else {
			return a.intValue() + b.intValue();
		}
	}
	
	class StDev extends Number{
		//https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
		private int count;
		private double mean;
		private double M2;
	
		void put(Number b){
			count += 1;
		    double delta = b.doubleValue() - mean;
		    mean += delta / count;
		    double delta2 = b.doubleValue() - mean;
		    M2 += delta * delta2;
		}
		double get(){
		    if(count < 2)
		        return 0;
		    else
		    	return M2 / (count - 1);
		}
		
		StDev combine(StDev other){
			int count = this.count + other.count;
			double delta = this.mean - other.mean;
			M2 = this.M2 + other.M2 + pow(delta, 2) * this.count * other.count / count;
		    this.count = count;
		    this.mean = (this.mean + other.mean)/2;
			return this;
		}
		
		@Override
		public int intValue() {
			return (int) get();
		}

		@Override
		public long longValue() {
			return (long) get();
		}

		@Override
		public float floatValue() {
			return (float) get();
		}

		@Override
		public double doubleValue() {
			return get();
		}
	}
	
	class Avg extends Number{
		private int count;
		private double sum;

		void put(Number newValue){
			count += 1;
		    sum += newValue.doubleValue(); 
		}
		double get(){
		    if(count < 1)
		        return 0;
		    else
		    	return  sum / count;
		}
		
		void combine(Avg other) {
			 this.count += other.count;
			 this.sum += (this.sum + other.sum)/2; 
		}
		
		@Override
		public int intValue() {
			return (int) get();
		}

		@Override
		public long longValue() {
			return (long) get();
		}

		@Override
		public float floatValue() {
			return (float) get();
		}

		@Override
		public double doubleValue() {
			return get();
		}
	}
}
