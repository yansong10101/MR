package filtering;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenUsers {

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> repMap = new TreeMap<Integer, Text>();		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String reputation = parsed.get("Reputation");
			if (parsed == null || reputation == null) {
		        return;
			}
				
			repMap.put(Integer.parseInt(reputation), new Text(value));
		
		    if (repMap.size() > 10) {
		    	repMap.remove(repMap.firstKey());
		    }
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException {
		    for (Text val : repMap.values()) {
	            context.write(NullWritable.get(), val);
		    }
		}
	}
	
	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		    for (Text val : values) {
		    	context.write(NullWritable.get(), val);
		    }		
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "TopTenUsers");
		job.setJarByClass(TopTenUsers.class);
		
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
