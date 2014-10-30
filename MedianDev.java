public class MedianDev {

	public static class MedianMapper extends Mapper<Object, Text, Text, IntWritable>{
		private Text outputKey = new Text();
		private IntWritable outValue = new IntWritable();
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		@SuppressWarnings("deprecation")
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String date = parsed.get("CreationDate");
			String text = parsed.get("Text");
			
			if(date != null && text != null){
				int length = text.length();
				try {
					Date creationDate = frmt.parse(date);					
					outValue.set(length);
					String tempDate = (creationDate.getYear()+1900)+"-"+(creationDate.getMonth()+1)+"-"+(creationDate.getDate()+1);
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
	
	public static class MedianReducer extends Reducer<Text, IntWritable, Text, Comments>{
		private Comments outValue = new Comments();
		private ArrayList<Integer> commentlength = new ArrayList<Integer>();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			double ave = 0.0;
			double totalDev = 0.0;
			commentlength.clear();
			
			for(IntWritable val : values){
				sum+=val.get();
				++count;
				commentlength.add(val.get());
			}
			
			Collections.sort(commentlength);
			
			if(commentlength.size()%2 == 0){
				int index = commentlength.size()/2;
				outValue.setMedian((int)(commentlength.get(index-1)+commentlength.get(index))/2);
			}else{
				int index = commentlength.size()/2;
				outValue.setMedian(commentlength.get(index-1));
			}
			
			ave = sum/count;
			
			for(Integer val : commentlength){
				totalDev+=(val - ave) * (val - ave);				
			}
			
			outValue.setDeviation(Math.sqrt(totalDev/(commentlength.size())));
			
			context.write(key, outValue);
		}
	}

	public static class Comments implements Writable{
		private int median;
		private double deviation;
		
		public void setMedian(int m){
			median = m;
		}
		
		public void setDeviation(double d){
			deviation = d;
		}
		
		public int getMedian(){
			return median;
		}
		
		public double getDviation(){
			return deviation;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			// TODO Auto-generated method stub
			median = input.readInt();
			deviation = input.readDouble();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			// TODO Auto-generated method stub
			output.writeInt(median);
			output.writeDouble(deviation);
		}
		
		public String toString(){
			return median + "\t" + deviation;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization"); 
		Job job = new Job(conf, "MedianDev");
		
		job.setJarByClass(MedianDev.class);	
		
		job.setMapperClass(MedianMapper.class);
		//job.setCombinerClass(ReducerTwo.class);
		job.setReducerClass(MedianReducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Comments.class);		
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		job.waitForCompletion(true);
		
	}

}
