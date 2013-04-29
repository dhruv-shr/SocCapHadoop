package mapreduce;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.Logger;

import infrastructure.BPMessageKey;
import infrastructure.GenericValue;
import infrastructure.ReverseMessage;
import infrastructure.VertexValue;
import io.VertexAdjacencyListReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import core.CommonConstants;

import util.GenericUtil;

public class ReversePropagationMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static Logger LOG = Logger.getLogger(ForwardPropagationMapper.class
			.getName());

	@Override
	public void map(LongWritable key, Text value, Context context) {

		int currentIteration = context.getConfiguration().getInt(
				CommonConstants.CURRENT_ITERATION, -1);
		
		double SCV = 0;
		 if(currentIteration==0){
			 
		try {
			
		
			VertexValue vertexValue = GenericUtil.getVertexObject(value
					.toString());
			Map<Integer, Integer> hopPacketCountMap = vertexValue
					.getHopPacketCountMap();
			Map<BPMessageKey, Integer> activeIncomingEdges = vertexValue
					.getActiveIncomingEdges();

			for (Integer hops : hopPacketCountMap.keySet()) {
				for (BPMessageKey bpMes : activeIncomingEdges.keySet()) {
					if (bpMes.hop == hops) {

						
						ReverseMessage reverseMessage = new ReverseMessage(
								bpMes.sourceNodeId,
								vertexValue.getDiscoveryDistance(), hops - 1,
								activeIncomingEdges.get(bpMes), hops);
						
						//Just for SCV Computation
						ReverseMessage reverseMessage1 = new ReverseMessage(
								bpMes.sourceNodeId,
								vertexValue.getDiscoveryDistance(), hops ,
								activeIncomingEdges.get(bpMes), hops);
						
						SCV = SCV + ReverseMessage.computeSCV(reverseMessage1);

					if(reverseMessage.getDistance()  >= 0 &&
					    reverseMessage.getHops() >= 0 &&
					    reverseMessage.getPackets() >=0 ){	
						// Message Emission For the
						context.write(
								new Text("" + bpMes.sourceNodeId),
								new Text(ReverseMessage
										.getMessageString(reverseMessage)));
					}

					}
				}
			}
			
			//Compute its one time SCV value;
			vertexValue.setScv(vertexValue.getScv() + SCV);

			
			context.write(new Text("" + vertexValue.getVertexID()), new Text(
					GenericUtil.getVertexValueString(vertexValue)));

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			System.out.println(exceptionAsString);
		}
		
		}else{
			// Current Iteration is not 0. This is the second time . IT will listen to all messages
			// and will broadcast it further.
			try{
				// Since key would be <source-Id> only.
				
				//Get the Key From the Map.
			    String genericMessage = value.toString();
			    String newKey = genericMessage.substring(0, genericMessage.indexOf(CommonConstants.TAB));
				String newValue = genericMessage.substring(genericMessage.indexOf(CommonConstants.TAB)+1);
			    	
			    context.write(new Text(newKey), new Text(newValue));
			    
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				String exceptionAsString = sw.toString();
				System.out.println(exceptionAsString);
			}
		}

	}

	public static void main(String args[]) throws Exception {
		String value = "7	true	6.0	0.0	{5,1.0,6,1.0}	{5,3,1,5,4,2,6,3,1,6,4,2}	{3,2,4,4}";
		VertexValue vertexValue = GenericUtil.getVertexObject(value);
		Map<Integer, Integer> hopPacketCountMap = vertexValue
				.getHopPacketCountMap();
		Map<BPMessageKey, Integer> activeIncomingEdges = vertexValue
				.getActiveIncomingEdges();

		for (Integer hops : hopPacketCountMap.keySet()) {
			for (BPMessageKey bpMes : activeIncomingEdges.keySet()) {
				if (bpMes.hop == hops) {

					ReverseMessage reverseMessage = new ReverseMessage(
							bpMes.sourceNodeId,
							vertexValue.getDiscoveryDistance(), hops - 1,
							activeIncomingEdges.get(bpMes), hops);
					System.out.println(bpMes.sourceNodeId + "$$"
							+ ReverseMessage.getMessageString(reverseMessage));

				}
			}
		}

	}
}
