package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import infrastructure.EmitInterface;
import infrastructure.EmitType;
import infrastructure.GenericValue;
import infrastructure.Message;
import infrastructure.VertexValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import util.GenericUtil;

import core.CommonConstants;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class ForwardPropagationMapper extends
		Mapper<IntWritable, VertexValue, IntWritable, GenericValue> {

	private static Logger LOG = Logger.getLogger(ForwardPropagationMapper.class
			.getName());

	@Override
	public void map(IntWritable vertexId, VertexValue vertexValue,
			Context context) {
		/*
		 * The mapper basically checks if a vertex is active then send a message
		 * to all of it's adjacent vertices. Note here that along with the
		 * message the vertex value also has to be emitted because then only the
		 * reducer will be able to combine all the incoming messages to a
		 * vertex.
		 */
		int sourceVertexId = context.getConfiguration().getInt(
				CommonConstants.SOURCE_VERTEX_ID, -1);
		int currentIteration = context.getConfiguration().getInt(
				CommonConstants.CURRENT_ITERATION, -1);

		if (sourceVertexId == vertexId.get() && currentIteration == 0) {
			/**
			 * The current vertex is the source vertex and this is the 0th
			 * iteration, and we need to do the appropriate initializations
			 * here.
			 */
			try {
				/* adding a message to itself */
				Message message = new Message(vertexId.get(), 0, 1, 0);
				/* emitting both the message and the vertex value */
				context.write(vertexId, GenericUtil.makeGeneric(message));
				context.write(vertexId, GenericUtil.makeGeneric(vertexValue));
			} catch (IOException e) {
				LOG.info(e.getMessage());
			} catch (InterruptedException e) {
				LOG.info(e.getMessage());
			}
		} else if (currentIteration == 0 && sourceVertexId != vertexId.get()) {
			/**
			 * This vertex is not the source vertex and hence we will simply say
			 * that the vertex is inactive and emit the vertex and the activity
			 * state.
			 */
			try {
				context.write(vertexId, GenericUtil.makeGeneric(vertexValue));
			} catch (IOException e) {
				LOG.info(e.getMessage());
			} catch (InterruptedException e) {
				LOG.info(e.getMessage());
			}
		} else if (vertexValue.isActive()
				&& currentIteration <= CommonConstants.HOPS_THRESHOLD) {
			/**
			 * The vertex is active that means it received some messages in the
			 * last reduce step and it will propogate further as long as the
			 * current iterations are lesser than the threshold iterations
			 * specified by the user. Hence propagate those messages further.
			 */
			int packets = vertexValue.getHopPacketCountMap().get(
					currentIteration - 1);
			/**
			 * Propagation Message to all adjacent nodes.
			 */
			for (Iterator<Integer> adjacentNodeIter = vertexValue
					.getAdjacencyList().keySet().iterator(); adjacentNodeIter
					.hasNext();) {
				int adjacentNodeId = adjacentNodeIter.next();
				double distance = vertexValue.getDiscoveryDistance() + 1.0
						/ (vertexValue.getAdjacencyList().get(adjacentNodeId));
				try {
					Message message = new Message(vertexId.get(),
							currentIteration, packets, distance);
					context.write(new IntWritable(adjacentNodeId), GenericUtil
							.makeGeneric(message));
				} catch (IOException e) {
					LOG.info(e.getMessage());
				} catch (InterruptedException e) {
					LOG.info(e.getMessage());
				}
			}
			try {
				context.write(vertexId, GenericUtil.makeGeneric(vertexValue));
			} catch (IOException e) {
				LOG.info(e.getMessage());
			} catch (InterruptedException e) {
				LOG.info(e.getMessage());
			}
		}

	}

}
