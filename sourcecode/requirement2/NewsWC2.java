package example;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class NewsWC2{
	/*
	 * 总体思路类似于NewsWC，包括两个mapreduce
	 * 第一部分的mapreduce应该实现将所有的（词，股票，词频，[urls]）输出
	 * map结果应为<词+股票，url+1>,reduce对后者进行合并,并将输出结果调整为<词+词频,股票+urls>
	 * 第二部分的mapreduce对第一部分的结果进行排序，对所有词分别对词频排序
	 * map读出第一部分结果，此处将键修改为一个新的WritableComparable类，输出即可，无需reduce
	 * 排序通过重写compareTo方法
	 */
	public static class NewsWC2Mapper extends Mapper<LongWritable,Text,Text,Text>{
		private Text wordkey = new Text();
		private Text wordvalue = new Text();
		private String pattern = "[^a-zA-Z0-9\u4e00-\u9fa5 -/\\.:]";//用于去除所有非法字符
		
		public void map(LongWritable offset,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString().replace("\t", "  ");//统一分隔符
			line = line.replaceAll(pattern, "");//去除错误数据，乱码
			String[] linesplit = line.split("  ");//标准数据中，分割符为双空格，结果依次为股票，日期，时间，标题，网址。标题需分词
			if(linesplit.length == 5) {
				wordvalue.set(linesplit[4]+",1");
				List<Word> words = WordSegmenter.seg(linesplit[3]);
				for(Word w:words) {
					wordkey.set(w.getText()+","+linesplit[0]);
					context.write(wordkey, wordvalue);
				}
			}
			else if(linesplit.length == 6) {
				wordvalue.set(linesplit[5]+",1");
				List<Word> words = WordSegmenter.seg(linesplit[4]);
				for(Word w:words) {
					wordkey.set(w.getText()+","+linesplit[0]);
					context.write(wordkey, wordvalue);
				}
			}
		}
	}
	
	public static class NewsWC2Reducer extends Reducer<Text,Text,Text,Text>{
		private Text newkey = new Text();
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			String urls = "";
			for (Text val:values) {
				String[] v = val.toString().split(",");
				sum += Integer.parseInt(v[1]);
				urls += v[0]+" ";
			}
			String[] k = key.toString().split(",");
			if(k.length == 2) {
				newkey.set(k[0]+" "+Integer.toString(sum));
				result.set(k[1]+","+urls);
				context.write(newkey, result);
			}
		}
	}
	
	public static class SortMapper extends Mapper<Text,Text,NewText,Text>{
		//将key格式由Text转换为NewText
		private NewText newkey = new NewText();
		public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
			newkey.set(key.toString());
			context.write(newkey, value);
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.err.println("Usage: NewsWC2 <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path tempPath = new Path(args[1]+"1");
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(tempPath))
			fs.delete(tempPath,true);
		Job job = Job.getInstance(conf,"newswc2");
		job.setJarByClass(NewsWC2.class);
		job.setMapperClass(NewsWC2Mapper.class);
		job.setReducerClass(NewsWC2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tempPath);
		
		if(job.waitForCompletion(true)) {
			if(fs.exists(outputPath))
				fs.delete(outputPath,true);
			Job sortjob = Job.getInstance(conf,"sort");
			sortjob.setJarByClass(NewsWC2.class);
			sortjob.setInputFormatClass(KeyValueTextInputFormat.class);
			sortjob.setMapperClass(SortMapper.class);
			sortjob.setNumReduceTasks(1);
			sortjob.setOutputKeyClass(NewText.class);
			sortjob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(sortjob, tempPath);
			FileOutputFormat.setOutputPath(sortjob, outputPath);
			System.exit(sortjob.waitForCompletion(true)?0:1);
		}
	}
}
