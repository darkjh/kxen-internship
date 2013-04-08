package com.kxen.han.projection.pig;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.Accumulator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Counter extends SimpleEvalFunc<DataBag> 
implements Accumulator<DataBag> {
	
	private final Long limit;
	private HashMap<Tuple, Long> counter;
	private DataBag outputBag;
	private Long one = 1L;
	
	
	public Counter() {
		this("0");
	}
	
	public Counter(String l) {
		this.limit = Long.parseLong(l);  
	}
	
	public void count(DataBag inputBag) {
		for (Tuple t : inputBag) {
			Long count = counter.containsKey(t) ? counter.get(t) : 0;
			counter.put(t, count + one);
		}
		for (Tuple t : counter.keySet()) {
			Long c = counter.get(t);
			if (c >= limit) {
				Tuple nt = TupleFactory.getInstance().newTuple(t.getAll());
				nt.append(counter.get(t));
				outputBag.add(nt);
			}
		}
	}
	
	public DataBag call(DataBag inputBag) throws IOException {
		cleanup();
		count(inputBag);
		return getValue();
	}
	
	@Override
	public void accumulate(Tuple arg0) throws IOException {
		DataBag inputBag = (DataBag) arg0.get(0);
		count(inputBag);
	}

	@Override
	public void cleanup() {
		counter = new HashMap<Tuple, Long>();
		outputBag = BagFactory.getInstance().newDefaultBag();
	}

	@Override
	public DataBag getValue() {
		return outputBag;
	}
}
