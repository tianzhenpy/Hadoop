package SouGou;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyticDec {
    /**
     *
     * @param targ
     * @param html
     * @return xmltxt
     * 解析xml的文本，并分割固定长度，返回长度
     */

    public static List<String> getList(int targ, String html) {
        List<String> mList = new ArrayList<>();
        List<String> xmltxt = new ArrayList<>();
        List<List<String>> mEndList = new ArrayList<>();
        Pattern p = Pattern.compile(">(.*)</");
        Matcher m = p.matcher(html);//开始编译
        while (m.find()) {
            mList.add(m.group(1));//获取被匹配的部分
        }
        if (mList.size() % targ != 0) {
            for (int j = 0; j < mList.size() / targ + 1; j++) {
                if ((j * targ + targ) < mList.size()) {
                    mEndList.add(mList.subList(j * targ, j * targ + targ));//0-3,4-7,8-11    j=0,j+3=3   j=j*3+1
                } else if ((j * targ + targ) > mList.size()) {
                    mEndList.add(mList.subList(j * targ, mList.size()));
                } else if (mList.size() < targ) {
                    mEndList.add(mList.subList(0, mList.size()));
                }
            }
        } else if (mList.size() % targ == 0) {
            for (int j = 0; j < mList.size() / targ; j++) {
                if ((j * targ + targ) <= mList.size()) {
                    mEndList.add(mList.subList(j * targ, j * targ + targ));//0-3,4-7,8-11    j=0,j+3=3   j=j*3+1
                } else if ((j * targ + targ) > mList.size()) {
                    mEndList.add(mList.subList(j * targ, mList.size()));
                } else if (mList.size() < targ) {
                    mEndList.add(mList.subList(0, mList.size()));
                }
            }
        }
        for (int i = 0; i < mEndList.size(); i++) {
            xmltxt.add(mEndList.get(i)+"");
        }
        return xmltxt;
    }
}
