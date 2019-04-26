/**
 * 
 */


import com.qianxinyao.analysis.jieba.keyword.Keyword;
import com.qianxinyao.analysis.jieba.keyword.TFIDFAnalyzer;

import java.util.List;

/**
 * @author qianxinyao
 * @email tomqianmaple@gmail.com
 * @github https://github.com/bluemapleman
 * @date 2016年10月23日
 */
public class TFIDFNEW
{

	/**
	 * 
	 * @param content 文本内容
	 * @param keyNums 返回的关键词数目
	 * @return
	 */
	public static List<Keyword> getTFIDE(String content, int keyNums)
	{
		TFIDFAnalyzer tfidfAnalyzer=new TFIDFAnalyzer();
		return tfidfAnalyzer.analyze(content,keyNums);
	}
}
