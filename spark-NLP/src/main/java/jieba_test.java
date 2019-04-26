import com.huaban.analysis.jieba.JiebaSegmenter;
import com.qianxinyao.analysis.jieba.keyword.Keyword;
import com.qianxinyao.analysis.jieba.keyword.TFIDFAnalyzer;

import java.util.List;

public class jieba_test {
    public static void main(String[] args) {
        String content="Abstracts of Chinese Geological Literature";
        int topN=5;
        TFIDFAnalyzer tfidfAnalyzer=new TFIDFAnalyzer();
        List<Keyword> list=tfidfAnalyzer.analyze(content,topN);
        for(Keyword word:list)
            System.out.println(word.getName()+":"+word.getTfidfvalue()+",");
    }
}
