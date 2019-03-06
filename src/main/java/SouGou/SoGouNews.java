package SouGou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SoGouNews extends Mapper<Object, Text, NullWritable,Text> {

    private List<String> mns,nes = new ArrayList<>();
    private static Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        mns = AnalyticDec.getList(4, val);
        for (int i = 0; i < mns.size(); i++) {
            nes = Collections.singletonList(mns.get(i));
            word.set(nes.get(0));
            context.write(NullWritable.get(),word);
        }
    }
}

class SogouMain{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"SogouMain");
        job.setJarByClass(SogouMain.class);
        job.setMapperClass(SoGouNews.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}