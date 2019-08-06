//package Common

object CommonObj {

  /**
    * 用户日志对象集合
    * @param vt 来访时间戳	visittime
    * @param un 用户名	username
    * @param gki 匿名用户的cookie id
    * @param ac 行为类型	action
    * @param ro 资源对象	resourceobject
    * @param klc 资源所属kbase库	kbaselibcode
    * @param ri 资源ID	resourceid
    * @param rkd 关键词	resourcekeyword
    * @param sw 检索词	searchword
    * @param p 价格	price
    * @param ci IP	clientip
    * @param ua UserAgent	useragent
    * @param rcc 资源标准分类	resoureclasscode
    * @param rccc 资源自定义分类	resourecustomclasscode
    * @param pfc 平台代码	platformcode
    * @param dt 设备类型	devicetype
    * @param di 移动设备ID	deviceid
    * @param au 相关作者 author
    * @param jg 机构
    * @param sou 来源
    */
  case class UserLogObj(vt:String,un:String,gki:String,ac:String,ro:String,klc:String,ri:String,rkd:String,sw:String,p:String,ci:String,ua:String,rcc:String,rccc:String,pfc:String,dt:String,di:String,au:String,jg:String,sou:String)

  /**
    * 用户画像内容临时对象（暂时无用）
    * @param UserID
    * @param UserName
    * @param prefList
    * @param prefListExtend
    * @param latest_log_time
    */
  case class UserTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedHashMap[String, Double]],latest_log_time:String)

  /**
    * 用户画像内容临时对象 (最新)
    * @param UserID
    * @param UserName
    * @param prefList
    * @param prefListExtend
    * @param latest_log_time
    */
  case class UserTempNew(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedHashMap[String, CustomizedKeyWord]],latest_log_time:String)

  /**
    * 用户关注学科维度临时对象
    * @param UserID
    * @param UserName
    * @param prefList
    * @param prefListExtend
    * @param latest_log_time
    */
  case class UserConcernedSubjectTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedKeyWord],latest_log_time:String)

  /**
    * 用户文章关键词维度临时对象（已弃用）
    * @param UserID
    * @param UserName
    * @param prefList
    * @param prefListExtend
    * @param latest_log_time
    */
  case class UserArticleTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, Double],latest_log_time:String)

  /**
    * 用户文章关键词维度临时对象（最新）
    * @param UserID
    * @param UserName
    * @param prefList
    * @param prefListExtend
    * @param latest_log_time
    */
  case class UserArticleTempNew(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String,CustomizedHashMap[String, CustomizedKeyWord]],latest_log_time:String)

  /**
    * 日志临时对象（guest-main方法引用）
    * @param username
    * @param view_time
    * @param title
    * @param content
    * @param module_id
    * @param keywords
    * @param map
    */
  case class Log_Temp(username:String,view_time:String,title:String,content:String,module_id:String,keywords:String,map:CustomizedHashMap[String, Double])

  /**
    * 用户画像类（已弃用）
    * @param UserID
    * @param UserName
    * @param LawOfworkAndRest
    * @param Area
    * @param Age
    * @param Gender
    * @param SingleArticleInterest
    * @param BooksInterests
    * @param JournalsInterests
    * @param ReferenceBookInterests
    * @param CustomerPurchasingPowerInterests
    * @param ProductviscosityInterests
    * @param PurchaseIntentionInterests
    * @param latest_log_time
    */
  case class users(UserID:BigInt,UserName:String,LawOfworkAndRest:String,Area:String,Age:Int,Gender:Int,SingleArticleInterest:String,BooksInterests:String,JournalsInterests:String,ReferenceBookInterests:String,CustomerPurchasingPowerInterests:String,ProductviscosityInterests:String,PurchaseIntentionInterests:String,latest_log_time:String)

  /**
    * 用户画像类（最新）
    * @param UserName
    * @param LawOfworkAndRest
    * @param Area
    * @param Age
    * @param Gender
    * @param ConcenedSubject
    * @param SubConcenedSubject
    * @param SingleArticleTotalInterest
    * @param SingleArticleRecentInterest
    * @param TotalRelatedAuthor
    * @param RecentRelatedAuthor
    * @param SearchKeyword
    * @param JournalsInterests
    * @param ReferenceBookInterests
    * @param CustomerPurchasingPowerInterests
    * @param latest_log_time
    */
  case class usersNew(UserName:String,LawOfworkAndRest:String,Area:String,Age:Int,Gender:Int,ConcenedSubject:String,SubConcenedSubject:String,SingleArticleTotalInterest:String,SingleArticleRecentInterest:String,TotalRelatedAuthor:String,RecentRelatedAuthor:String,SearchKeyword:String,JournalsInterests:String,ReferenceBookInterests:String,CustomerPurchasingPowerInterests:String,latest_log_time:String)

  /**
    * 保存至mysql的用户画像类
    * @param UserName
    * @param LawOfworkAndRest
    * @param Area
    * @param Age
    * @param Gender
    * @param ConcenedSubject
    * @param SubConcenedSubject
    * @param SingleArticleTotalInterest
    * @param SingleArticleRecentInterest
    * @param TotalRelatedAuthor
    * @param RecentRelatedAuthor
    * @param SearchKeyword
    * @param JournalsInterests
    * @param ReferenceBookInterests
    * @param CustomerPurchasingPowerInterests
    * @param hasPortrait
    * @param latest_log_time
    */
  case class usersToMysql(UserName:String,LawOfworkAndRest:String,Area:String,Age:Int,Gender:Int,ConcenedSubject:String,SubConcenedSubject:String,SingleArticleTotalInterest:String,SingleArticleRecentInterest:String,TotalRelatedAuthor:String,RecentRelatedAuthor:String,SearchKeyword:String,JournalsInterests:String,ReferenceBookInterests:String,CustomerPurchasingPowerInterests:String,hasPortrait:Int,latest_log_time:String)

  /**
    * 基于内容的推荐所相关标签（暂时无用）
    * @param UserName
    * @param TotalRelatedAuthor
    * @param RecentRelatedAuthor
    */
  case class RelatedLabel(UserName:String,TotalRelatedAuthor:String,RecentRelatedAuthor:String)

  /**
    * 基于内容的推荐所用到基础数据临时对象
    * @param id
    * @param content
    * @param news_time
    * @param title
    * @param module_id
    * @param keywords
    */
  case class NewsTemp(id:Long, content:String, news_time:String, title:String, module_id:String, keywords:CustomizedHashMap[String, Double])

  /**
    * 图书行为日志
    * @param username
    * @param id
    * @param class_code
    * @param keywords
    */
  case class BookLogInfo(username:String,id:String,class_code:String,keywords:String)

  /**
    * 基于内容的推荐生成的推荐类（暂时无用）
    * @param user_id
    * @param username
    * @param news_id
    * @param news_title
    * @param score
    */
  case class recommendations(user_id:BigInt,username:String,news_id:BigInt,news_title:String,score:Double)
}
