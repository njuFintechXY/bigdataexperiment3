package example;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class NewsWC{
	/*
	 * 总体上和WordCount程序类似，不同之处在于中文文本需要先分词
	 * 去除停用词在map中实现即可(分词包中有相应方法）
	 * 结果输出后
	 * 输出需按词频从高到低输出，需自定义comparator类，并执行一个sort的mapreduce程序
	 */
	
	public static class NewsWCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String pattern = "[^\u4e00-\u9fa5]";
		public void map(LongWritable offset, Text value, Context context) throws IOException,InterruptedException{
			String line = value.toString();
			line = line.replaceAll(pattern, "");
			
			List<Word> words = WordSegmenter.seg(line);
			for(Word w:words) {
				word.set(w.getText());
				context.write(word, one);
			}
		}
	}
	
	//以下代码来源于书P118,增加了K次以上这个限制
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		private int k;
		protected void setup(Context context) {
			k = context.getConfiguration().getInt("k", 100);
		}
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for (IntWritable val:values) {
				sum += val.get();
			}
			if(sum >= k) {
				result.set(sum);
				context.write(key,result);
			}
		}
	}
	
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator{
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1,l1,b2,s2,l2);
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage: NewsWC <k> <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		conf.setInt("k", Integer.parseInt(args[0]));
		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		Path tempPath = new Path(args[2]+"1");
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(tempPath))
			fs.delete(tempPath, true);
		Job job = Job.getInstance(conf,"NewsWC");
		job.setJarByClass(NewsWC.class);
		job.setMapperClass(NewsWCMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tempPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if(job.waitForCompletion(true)) {
			if(fs.exists(outputPath))
				fs.delete(outputPath,true);
			Job sortJob = Job.getInstance(conf,"sort");
			sortJob.setJarByClass(NewsWC.class);
			sortJob.setMapperClass(InverseMapper.class);
			FileInputFormat.addInputPath(sortJob, tempPath);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			sortJob.setNumReduceTasks(1);
			FileOutputFormat.setOutputPath(sortJob, outputPath);
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(Text.class);
			sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
			System.exit(sortJob.waitForCompletion(true)?0:1);
		}
	}
}





