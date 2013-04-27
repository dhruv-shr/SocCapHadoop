package io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import infrastructure.BPMessageKey;
import infrastructure.VertexValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import core.CommonConstants;

/**
 * 
 * @author dhruvsharma1
 * 
 */
public class VertexAdjacencyListReader extends
		RecordReader<IntWritable, VertexValue> {

	private LineRecordReader lineReader = new LineRecordReader();
	private String[] splits;

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		splits = lineReader.getCurrentValue().toString().split(
				CommonConstants.TAB);
		return new IntWritable(Integer.parseInt(splits[0]));
	}

	@Override
	public VertexValue getCurrentValue() throws IOException,
			InterruptedException {
		/**
		 * vertex_id <tab> splits[0] active <tab> splits[1] discoveryDistance
		 * <tab> splits[2] scv <tab> splits[3] {vid,wt,vid,wt} <tab> splits[4]
		 * {src,hops,pkts,src,hops,pkts} <tab> splits[5] {hops,pkts,hops,pkts}
		 * <new-line> splits[6]
		 */
		splits = lineReader.getCurrentValue().toString().split(
				CommonConstants.TAB);
		VertexValue vertexValue = new VertexValue();
		vertexValue.setActive(Boolean.parseBoolean(splits[1]));
		vertexValue.setDiscoveryDistance(Double.parseDouble(splits[2]));
		vertexValue.setScv(Double.parseDouble(splits[3]));
		int start = splits[4].trim().indexOf("{");
		int stop = splits[4].trim().indexOf("}");
		String adjListString = splits[4].substring(start + 1, stop);
		Map<Integer, Double> adjacencyList = new HashMap<Integer, Double>();
		if (!adjListString.isEmpty()) {
			String[] adjList = adjListString.split(",");
			for (int i = 0; i < adjList.length; i += 2) {
				adjacencyList.put(Integer.parseInt(adjList[i]), Double
						.parseDouble(adjList[i + 1]));
			}
		}
		vertexValue.setAdjacencyList(adjacencyList);
		start = splits[5].trim().indexOf("{");
		stop = splits[5].trim().indexOf("}");
		String bpMsgString = splits[5].substring(start + 1, stop);
		Map<BPMessageKey, Integer> bpMsgList = new HashMap<BPMessageKey, Integer>();
		if (!bpMsgString.isEmpty()) {
			String[] bpMsgListString = bpMsgString.split(",");
			for (int i = 0; i < bpMsgListString.length; i += 3) {
				BPMessageKey k = new BPMessageKey(Integer
						.parseInt(bpMsgListString[i]), Integer
						.parseInt(bpMsgListString[i + 1]));
				bpMsgList.put(k, Integer.parseInt(bpMsgListString[i + 2]));
			}
		}
		vertexValue.setActiveIncomingEdges(bpMsgList);

		start = splits[6].trim().indexOf("{");
		stop = splits[6].trim().indexOf("}");
		String hopPktCntString = splits[6].substring(start + 1, stop);
		Map<Integer, Integer> hopPktCntMap = new HashMap<Integer, Integer>();
		if (!hopPktCntString.isEmpty()) {
			String[] hpcString = hopPktCntString.split(",");
			for (int i = 0; i < hpcString.length; i += 2) {
				int k = Integer.parseInt(hpcString[i]);
				hopPktCntMap.put(k, Integer.parseInt(hpcString[i + 1]));
			}
		}
		vertexValue.setHopPacketCountMap(hopPktCntMap);

		return vertexValue;

	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		lineReader.initialize(inputSplit, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lineReader.nextKeyValue();
	}

}
