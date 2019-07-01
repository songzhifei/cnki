//package Common

object CommonObj {
  /*
*
描述        代码      缩写
来访时间戳	visittime	vt
用户名	username	un
匿名用户的cookie id	guestidcookie	gki
行为类型	action	ac
资源对象	resourceobject	ro
资源所属kbase库	kbaselibcode	klc
资源ID	resourceid	ri
关键词	resourcekeyword	rkd
检索词	searchword	sw
价格	price	p
IP	clientip	ci
UserAgent	useragent	ua
资源标准分类	resoureclasscode	rcc
资源自定义分类	resourecustomclasscode	rccc
平台代码	platformcode	pfc
设备类型	devicetype	dt
移动设备ID	deviceid	di
*
* */
  case class UserLogObj(vt:String,un:String,gki:String,ac:String,ro:String,klc:String,ri:String,rkd:String,sw:String,p:String,ci:String,ua:String,rcc:String,rccc:String,pfc:String,dt:String,di:String)



  case class UserTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, CustomizedHashMap[String, Double]],latest_log_time:String)

  case class UserArticleTemp(UserID:BigInt,UserName:String, prefList:String,prefListExtend:CustomizedHashMap[String, Double],latest_log_time:String)

  case class Log_Temp(username:String,view_time:String,title:String,content:String,module_id:String,map:CustomizedHashMap[String, Double])

  case class users(UserID:BigInt,UserName:String,LawOfworkAndRest:String,Area:String,Age:Int,Gender:Int,SingleArticleInterest:String,BooksInterests:String,JournalsInterests:String,ReferenceBookInterests:String,CustomerPurchasingPowerInterests:String,ProductviscosityInterests:String,PurchaseIntentionInterests:String,latest_log_time:String)

  case class UserPortrait(UserID:BigInt,UserName:String,LawOfworkAndRest:Int,Area:String,Age:Int,Gender:Int,SingleArticleInterest:String)

  case class NewsTemp(id:Long, content:String, news_time:String, title:String, module_id:String, keywords:CustomizedHashMap[String, Double])

  case class JournalBaseTemp(id:Long,PYKM:String, content:String, news_time:String, title:String, module_id:String, keywords:String)

  case class BookBaseInfo(id:String,title:String,content:String,class_code:String,keywords:String)

  case class BookLogInfo(username:String,id:String,class_code:String,keywords:String)

  case class recommendations(user_id:BigInt,username:String,news_id:BigInt,news_title:String,score:Double)
}
