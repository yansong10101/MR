public class NumberComments {
	public static class NumberMapper extends Mapper<Object, Text, Text, IntWritable>{
		private Text outputKey = new Text();
		private IntWritable outValue = new IntWritable();
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		@SuppressWarnings("deprecation")
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String date = parsed.get("CreationDate");
			IntWritable one = new IntWritable(1);
			
			if(date != null){
				try {
					Date creationDate = frmt.parse(date);					
					outValue = one;
					String tempDate = (creationDate.getYear()+1900)+"-"+(creationDate.getMonth()+1)+"-"+creationDate.getDate();
					outputKey.set(tempDate);
					
					context.write(outputKey, outValue);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}else{
				return;
			}			
		}
	}
	
	public static class NumberReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outSum = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			
			for(IntWritable val : values){
				++sum;				
			}
			
			outSum.set(sum);
			
			context.write(key, outSum);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization"); 
		Job job = new Job(conf, "NumberComments");
		
		job.setJarByClass(NumberComments.class);	
		
		job.setMapperClass(NumberMapper.class);
		//job.setCombinerClass(ReducerTwo.class);
		job.setReducerClass(NumberReducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		job.waitForCompletion(true);
		
	}

}
