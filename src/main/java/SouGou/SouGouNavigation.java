package SouGou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SouGouNavigation extends Mapper<Object, Text,Text, IntWritable> {
    private static Text word = new Text();
    private static IntWritable one = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] arg = value.toString().split("\\t");
        if (arg[arg.length - 1].equals("1") || arg[arg.length - 1].equals("2")) {
            word.set(arg[arg.length - 1]);
            context.write(word, one);
        }
    }
}

class SouGouNavReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    private static IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}

class SouGouNavMain{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"SouGouNavMain");
        job.setJarByClass(SouGouNavMain.class);
        job.setMapperClass(SouGouNavigation.class);
        job.setReducerClass(SouGouNavReduce.class);
        job.setCombinerClass(SouGouNavReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}