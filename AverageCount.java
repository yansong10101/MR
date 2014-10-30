public class AverageCount {
	public static class AverageMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		private Text outKey = new Text();
		private DoubleWritable outValue = new DoubleWritable();		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String userId = parsed.get("OwnerUserId");
			String viewCount = parsed.get("ViewCount");
			if(userId!=null && Double.parseDouble(viewCount)!=0){
				outKey = new Text(userId);				
				outValue = new DoubleWritable(Double.parseDouble(viewCount));
				context.write(outKey, outValue);
			}else{
				return;
			}
		}
	}	
	public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable average = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0.0;
			int count = 0;
			
			for(DoubleWritable val : values){
				sum+=val.get();
				++count;
			}
			average = new DoubleWritable(sum/count);
			
			context.write(key, average);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization"); 
		Job job = new Job(conf, "AverageCount");
		
		job.setJarByClass(AverageCount.class);		
		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		job.waitForCompletion(true);
	}

}
