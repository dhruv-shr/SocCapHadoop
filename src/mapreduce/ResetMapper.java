package mapreduce;

import infrastructure.VertexValue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import core.CommonConstants;

import util.GenericUtil;

public class ResetMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Logger LOG = Logger.getLogger(ResetMapper.class.getName());

	@Override
	public void map(LongWritable key, Text value, Context context) {

		
		try{
		  //Take the Value and value will be a vertex Object 
			String genericMessage = value.toString();
		    String newKey = genericMessage.substring(0, genericMessage.indexOf(CommonConstants.TAB));
			String newValue = genericMessage.substring(genericMessage.indexOf(CommonConstants.TAB)+1);
		    
		  VertexValue vertexValue = GenericUtil.getVertexObject(newValue.toString());
		     vertexValue.resetTemproryValues();
		     
		 context.write(new Text("" + newKey), new Text(
						GenericUtil.getVertexValueStringFinalPhase(vertexValue)));   
		
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			System.out.println(exceptionAsString);
		}
		
	}

}
