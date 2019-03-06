package DarkHorse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.ResourceBundle;

public class DarkHorseClean extends Mapper<Object, Text, NullWritable, Text> {

    private static Text word = new Text();
    private static ResourceBundle config = ResourceBundle.getBundle("conf/conf.properties");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(" ");
        if (str[9].equals("-") || str[6].indexOf("static") == -1 || str[6].indexOf("uc_server") == -1) {
            String ip = str[0];
            String time = str[3];
            time = time.replace("[", "");
            SimpleDateFormat in = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
            SimpleDateFormat out = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss");
            try {
                in.parse(time);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String url = str[6];
            StringBuffer sb = new StringBuffer();
            sb.append(ip).append(config.getString("filesplit")).append(time).append(config.getString("filesplit")).append(url);
            word.set(sb.toString());
            context.write(NullWritable.get(), word);
        }
    }
}

class DarkHorseMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DarkHorseMain");
        job.setJarByClass(DarkHorseMain.class);
        job.setMapperClass(DarkHorseClean.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

