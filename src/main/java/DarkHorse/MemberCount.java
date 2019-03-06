package DarkHorse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MemberCount extends Mapper<Object,Text, Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(" ");
        String s = str[1]+" "+str[2];
        if(s.indexOf("member.php?mod=register") == -1){
        }else {
        }
    }
}
