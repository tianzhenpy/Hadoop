package mr_job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


public class TProperties {

    private static Properties p = new Properties();

    /**
     * 读取properties配置文件信息 
     */
    static {
        try {
            //jar内获取配置文件
            InputStream is = TProperties.class.getResourceAsStream("/conf.properties");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            p.load(br);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据key得到value的值
     */
    public static String getValue(String key) {
        return p.getProperty(key);
    }

}
