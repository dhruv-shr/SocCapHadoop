package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import infrastructure.EmitInterface;
import infrastructure.EmitType;
import infrastructure.GenericValue;
import infrastructure.Message;
import infrastructure.VertexValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This reducer class basically combines all incoming messages to a vertex in an
 * iteration and writes down the new modified VertexValue object to be read by
 * the mapper in the next iterations.
 * 
 * @author dhruvsharma1
 * 
 */
public class ForwardPropagationReducer extends
		Reducer<IntWritable, GenericValue, IntWritable, VertexValue> {

	private static Logger LOG = Logger
			.getLogger(ForwardPropagationReducer.class.getName());

	/**
	 * Overriding the default reduce method implementation with ours.
	 */
	@Override
	public void reduce(IntWritable vertexId, Iterable<GenericValue> values,
			Context context) {
		/**
		 * Here the GenericValue object can be of 2 types : 1. Message 2.
		 * VertexValue. The job of the reducer is to combine all the incoming
		 * messages emitted by the mapper of current iteration together and mark
		 * the nodes to be active for the next iteration.
		 */
		VertexValue vertexValue = new VertexValue();
		List<Message> msgList = new ArrayList<Message>();
		while (values.iterator().hasNext()) {
			GenericValue val = values.iterator().next();
			EmitInterface ei = (EmitInterface) val;
			if (ei.getEmitType().equals(EmitType.VERTEX)) {
				vertexValue = (VertexValue) ei;
			} else {
				msgList.add((Message) ei);
			}
		}
		if (msgList.size() == 0) {
			/*
			 * no incoming messages hence just emit the vertex for the next
			 * iteration with active = false.
			 */
			vertexValue.setActive(false);
		} else {
			/*
			 * There are incoming messages for this vertex hence, we need to add
			 * messages to this vertex and set it to active for the mext map
			 * iteration.
			 */
			for (Iterator<Message> msgIter = msgList.iterator(); msgIter
					.hasNext();) {
				Message msg = msgIter.next();
				vertexValue.addIncomingMessage(msg);
				vertexValue.setActive(true);
			}
		}
		try {
			context.write(vertexId, vertexValue);
		} catch (IOException e) {
			LOG.info(e.getMessage());
		} catch (InterruptedException e) {
			LOG.info(e.getMessage());
		}
	}

}
