public class FileDivission {	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.get(conf);
		
		int numFiles = Integer.parseInt(args[1]);				
		int totalLen = (int)hdfs.getFileStatus(new Path(args[0])).getLen();
		int normalSize = getNormalSize(totalLen, numFiles);
		int largeSize = getLargeSize(totalLen, numFiles);		
		Path inputfile = new Path(args[0]);		
		
		FSDataInputStream in = hdfs.open(inputfile);
		byte buffer[] = new byte[normalSize];
		byte largeBuffer[] = new byte[largeSize];
		int byteRead = 0;
		int count = 0;
		
		ArrayList<FSDataOutputStream> out = new ArrayList<FSDataOutputStream>();
		for(int i = 0; i < numFiles; i++){
			Path tempPath = new Path("/home/yansong/Desktop/tempout/file"+i);
			out.add(local.create(tempPath));
		}
		
		while((byteRead = in.read(buffer)) > 0){
			if(count < out.size()){
				out.get(count).write(buffer);
				count++;
				System.out.println(byteRead);
			}
		}			
	}	
	
	public static int getNormalSize(int total, int num){
		return total/num;
	}
	
	public static int getLargeSize(int total, int num){
		return (total%num+total/num);
	}
}
