package filtering;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.net.URI;

public class UserCommentInfo {

	public static class UserinfoMapper extends	Mapper<Object, Text, IntWritable, Text> {

		private BloomFilter filter = new BloomFilter();

		protected void setup(Context context) throws IOException,InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			DataInputStream strm1 = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(strm1);
			strm1.close();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String userid = parsed.get("Id");
			
			if (null == userid) {
				return;
			}

			if ((filter.membershipTest(new Key(userid.getBytes())))) {
				context.write(new IntWritable(Integer.parseInt(userid)),new Text("U" + value.toString()));
			}
		}
	}

	public static class CommentinfoMapper extends Mapper<Object, Text, IntWritable, Text> {

		private BloomFilter filter = new BloomFilter();

		protected void setup(Context context) throws IOException,InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());			
//			System.out.println("Path" + files[0].getPath());			
			DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(strm);
			strm.close();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String userid = parsed.get("PostId");

			if (null == userid) {
				return;
			}

			if ((filter.membershipTest(new Key(userid.getBytes())))) {
				context.write(new IntWritable(Integer.parseInt(userid)),new Text("C" + value.toString()));
			}
		}
	}

	public static class UserCommentsReducer extends	Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			ArrayList<String> listComments = new ArrayList<String>();
			String userinfo = null;

			for (Text val : values) {
				if ('U' == val.toString().charAt(0)) {
					userinfo = val.toString().substring(1);
				} else {
					listComments.add(val.toString().substring(1));
				}
			}

			for (int i = 0; i < listComments.size(); i++) {
				context.write(new Text("Comment: " + listComments.get(i)),new Text("user : " + userinfo));
			}
		}
	}

	public static int getOptimalBloomFilterSize(int numRecords,float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		/* Bloomfilter init */
		int vectorSize = getOptimalBloomFilterSize(1000000, 0.1f);
		int nbHash = getOptimalK(1000000, vectorSize);
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,Hash.MURMUR_HASH);
		String line = null;

		// user path is args[0]
		// comment path is args[1]
		// output path is args[2]
		// Reputation value is args[3]
		// bloomfilter path is args[4]
		Path inputFile = new Path(args[0]);
		Path bfFile = new Path(args[4]);

		int iReputation = Integer.parseInt(args[3]);
		FileSystem fs = FileSystem.get(new Configuration());

		for (FileStatus status : fs.listStatus(inputFile)) {

			BufferedReader rdr = new BufferedReader(new InputStreamReader(new DataInputStream(fs.open(status.getPath()))));

			System.out.println("Reading " + status.getPath());
			
			while ((line = rdr.readLine()) != null) {
				Map<String, String> parsed = MRDPUtils.transformXmlToMap(line.toString());
				String userid = parsed.get("Id");
				
				if (null == userid) {
					continue;
				}

				int iRep = Integer.parseInt(parsed.get("Reputation"));

				if (iRep < iReputation) {
					continue;
				}

//				System.out.println("Build filter: userid = " + userid + " rep = " + iRep);

				filter.add(new Key(userid.getBytes()));
			}
			rdr.close();
		}

		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		
		System.out.println("init bloomfilter success");

		// move strm file into DistributedCache
		DistributedCache.addCacheFile(bfFile.toUri(), conf);

		Job job = new Job(conf, "UserCommentBlomfilter");
		job.setJarByClass(UserCommentInfo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// add user file
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, UserinfoMapper.class);
		// add comment file
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, CommentinfoMapper.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(UserCommentsReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}

}
