package mapreduce;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ResetReducer extends Reducer<Text, Text, Text, Text> {

	private static Logger LOG = Logger
			.getLogger(ForwardPropagationReducer.class.getName());

	/**
	 * Overriding the default reduce method implementation with ours.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {

		for (Text t : values) {
			try {
				context.write(key, t);
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				String exceptionAsString = sw.toString();
				System.out.println(exceptionAsString);
			}
		}
	}

}