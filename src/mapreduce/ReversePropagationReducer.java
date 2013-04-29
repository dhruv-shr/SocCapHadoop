package mapreduce;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import infrastructure.BPMessageKey;
import infrastructure.GenericValue;
import infrastructure.ReverseMessage;
import infrastructure.VertexValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import util.GenericUtil;

import core.CommonConstants;

public class ReversePropagationReducer extends Reducer<Text, Text, Text, Text> {

	private static Logger LOG = Logger
			.getLogger(ForwardPropagationReducer.class.getName());

	/**
	 * Overriding the default reduce method implementation with ours.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {

		List<String> ownData = new ArrayList<String>();
		List<String> messageData = new ArrayList<String>();

		// Seperate All the Incoming Messages.
		for (Text incomingMessage : values) {
			String genericMessage = incomingMessage.toString();
			String genericMessageSplitted[] = genericMessage
					.split(CommonConstants.TAB);
			if (genericMessageSplitted.length == 5) {
				messageData.add(genericMessage);
			} else {
				ownData.add(genericMessage);
			}
		}

		try {
			// Compute the SCV Values.
			double SCV = 0;
			for (String reverseMessage : messageData) {
				ReverseMessage reverseMessage2 = ReverseMessage
						.getReverseMessageObject(reverseMessage);
				SCV = SCV + ReverseMessage.computeSCV(reverseMessage2);

				// Start Outputting the Message Data.
				VertexValue vertexValue = GenericUtil.getVertexObject(ownData.get(0));
				Map<Integer, Integer> hopPacketCountMap = vertexValue
						.getHopPacketCountMap();
				Map<BPMessageKey, Integer> activeIncomingEdges = vertexValue
						.getActiveIncomingEdges();

				
					for (BPMessageKey bpMes : activeIncomingEdges.keySet()) {
						if (bpMes.hop == reverseMessage2.getHops()) {

							ReverseMessage reverseMessageObject = new ReverseMessage(
									bpMes.sourceNodeId,
									reverseMessage2.getDistance(), reverseMessage2.getHops() - 1,
									activeIncomingEdges.get(bpMes), reverseMessage2.getTotalHops());

							// Message Emission For the
							if(reverseMessageObject.getDistance()  >= 0 &&
									reverseMessageObject.getHops() >= 0 &&
											reverseMessageObject.getPackets() >=0 ){	
							context.write(
									new Text("" + bpMes.sourceNodeId),
									new Text(ReverseMessage
											.getMessageString(reverseMessageObject)));
							}

						}
					}
				}
			

			VertexValue vertexValue = null;

			// Get the Vertex Value Object.
			vertexValue = GenericUtil.getVertexObject(ownData.get(0));

			// Set the SCV Value, which was just computed.
			vertexValue.setScv(vertexValue.getScv() + SCV);

			// Output its Own Data.
			context.write(key,
					new Text(GenericUtil.getVertexValueString(vertexValue)));

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			System.out.println(exceptionAsString);
		}

	}

}
