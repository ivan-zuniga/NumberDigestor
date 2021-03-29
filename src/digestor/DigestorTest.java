package digestor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import digestor.Digestor.Opcode;

public class DigestorTest {

	Digestor digestor;
	Map result;
	
	// Datos de entrada
	final static Number[][] table = {{1,2,100}, {1,2,600}, {3,4,200},{5,6,300},{7,8,400},{9,2,500.5}, {9,20,500.5}};
	final static Number[] EOF = {};
	final static List<Number[]> list = Arrays.asList(table);
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		for (Number[] row : list) { System.out.println(Arrays.toString(row));}
	}

	@Before
	public void setUp() throws Exception {
		digestor = new Digestor();
	}
	
	@Test
	public void testGetGroupingByIntOpcode() {
		System.out.println("\ntestGetGroupingByIntOpcode\n");
	
		Digestor digestor = new Digestor();
	
		Opcode[] ops ={Opcode.COUNT, Opcode.SUM, Opcode.SUM};
		Map groupedBy0 = (Map) list.stream().collect(digestor.getGroupingBy(0, ops));	
		System.out.println("Group(0)" + Arrays.toString(ops));
		digestor.print(groupedBy0); System.out.println();
		
		Opcode[] ops1 ={Opcode.SUM, Opcode.COUNT, Opcode.AVG};
		Map groupedBy1 = (Map) list.stream().collect(digestor.getGroupingBy(1, ops1));	
		System.out.println("Group(1)" + Arrays.toString(ops1));
		digestor.print(groupedBy1);System.out.println();
		
		Opcode[] ops0b ={Opcode.COUNT, Opcode.MAX, Opcode.MIN};
		Map groupedBy0b = (Map) list.stream().collect(digestor.getGroupingBy(0, ops0b));	
		System.out.println("Group(0)" + Arrays.toString(ops0b));
		digestor.print(groupedBy0b); System.out.println();
		
		//TODO asserts to make it a test!
	}

	@Test
	public void testGetGroupingByIntCollectorOfNumberQMapOfNumberListOfNumber() {
		
		System.out.println("Group by column 0");
		Map<Number, List<Number[]>> groupedBy0 = list.stream().collect(Collectors.groupingBy( tuple-> tuple[0]));	
		Digestor.print(groupedBy0);
		System.out.println();
		
		System.out.println("Group by column 1");
		Map<Number, List<Number[]>> groupedBy1 = list.stream().collect(digestor.getGroupingBy(1));	
		Digestor.print(groupedBy1);
		System.out.println();
		
		System.out.println("Group by columns 0, 1");
		Collector<Number[], ?, Map<Number, Map<Number, List<Number[]>>>> grupingBy01 = digestor.getGroupingBy(0, digestor.getGroupingBy(1));
	    Map groupedBy01 = list.stream().collect(grupingBy01);	
	    
	    Digestor.print(groupedBy01);
		System.out.println();
		
		System.out.println("Group by columns 1, 0");
		Collector<Number[], ?, ?> grupingBy10 = digestor.getGroupingBy(1, digestor.getGroupingBy(0));
		Map groupedBy10 = (Map<?, ?>) list.stream().collect(grupingBy10);	
	    
		Digestor.print(groupedBy10);
		System.out.println();
		
		Collector<Number[], ?, ?> grupingBy012 = digestor.getGroupingBy(0, digestor.getGroupingBy(1, digestor.getGroupingBy(2)));
		Map groupedBy012 = (Map<?, ?>) list.stream().collect(grupingBy012);	
	    
		System.out.println("Group by columns 0, 1, 2");
		Digestor.print(groupedBy012);
		System.out.println();
	}
	
	@Test
	public void testGetContinuosStreamAggregating() throws InterruptedException {
		System.out.println("\ntestGetContinuosStreamAggregating\n");
		
		//Cola de entrada
		BlockingQueue<Number[]> queue = new LinkedBlockingQueue<Number[]>();

		// Stream consumiendo filas de la cola hasta recibir una fila vacia
		Stream<Number[]> stream = Digestor.getContinuosStream(queue);
	
		// El stream agrupando las filas por la segunda columa 
		Opcode[] ops1 ={Opcode.SUM, Opcode.COUNT, Opcode.AVG};
		Runnable task = ()-> result = (Map) stream.collect(digestor.getGroupingBy(1, ops1));
		Thread thread = new Thread(task);
		thread.start();
		
		// Produciendo filas y agregandolas a la cola 
		for(Number[] e: table) {queue.put(e);}
		queue.put(EOF);

		//Esperando e imprimiento el resultado
		thread.join();
		System.out.println("Group(1)" + Arrays.toString(ops1));
		Digestor.print(result);
		
		//TODO asserts to make it a test!
	}

	@Test
	public void testGetContinuosStreamGrouping() throws InterruptedException {
		System.out.println("\ntestGetContinuosStreamGrouping\n");
		
		//Cola de entrada
		BlockingQueue<Number[]> queue = new LinkedBlockingQueue<Number[]>();

		// Stream consumiendo filas de la cola hasta recibir una fila vacia
		Stream<Number[]> stream = Digestor.getContinuosStream(queue);
	
		// El stream agrupando las filas por la segunda columa 
		Runnable task = ()-> result = stream.collect(Collectors.groupingBy(row-> row[1]));
		Thread thread = new Thread(task);
		thread.start();
		
		// Produciendo filas y agregandolas a la cola 
		for(Number[] e: table) {queue.put(e);}
		for(Number[] e: table) {queue.put(e);}

		queue.put(EOF);

		//Esperando e imprimiento el resultado
		thread.join();
		Digestor.print(result);
		
		//TODO asserts to make it a test!
	}

	@Test
	public void testGetContinuosParallelStreamGrouping() throws InterruptedException {
		System.out.println("\ntestGetContinuosStreamGrouping\n");
		
		//Cola de entrada
		BlockingQueue<Number[]> queue = new LinkedBlockingQueue<Number[]>();

		// Stream consumiendo filas de la cola hasta recibir una fila vacia
		Stream<Number[]> stream = Digestor.getContinuosStream(queue);
	
		// El stream agrupando las filas por la segunda columa 
		//Runnable task = ()-> result = stream.parallel().collect(Collectors.groupingBy(row-> row[1]));
		
		// El stream agrupando las filas por la segunda columa 
		Opcode[] ops1 ={Opcode.SUM, Opcode.COUNT, Opcode.AVG};
		Runnable task = ()-> result = (Map) stream.parallel().collect(digestor.getGroupingBy(1, ops1));
		Thread thread = new Thread(task);
		thread.start();
		
		// Produciendo filas y agregandolas a la cola 
		for(int i = 0; i < 10; i++)
			for(Number[] e: table) {queue.put(e);}

		queue.put(EOF);

		//Esperando e imprimiento el resultado
		thread.join();
		Digestor.print(result);
		
		//TODO asserts to make it a test!
	}
	
	@Test
	public void testGetInfiniteStream() {
		//fail("Not yet implemented");
	}
}
