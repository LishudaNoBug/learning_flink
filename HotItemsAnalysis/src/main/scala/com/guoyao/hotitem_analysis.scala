package com.guoyao

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
商品实时浏览量TopN：
步骤
1. 定义输入数据样例类用于封装数据  687507,703558,1168596,pv,1511690400
2. 读取数据封装为样例类
3.

 */

/*UserBehavior：用于封装原始读入数据
userId：用户；  itemId：商品ID；  categoryId：商品分类ID；  behavior：用户pv、fav等操作； timestamp：用户操作时间
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/*ItemViewCount:用于封装最终输出的数据
itemId:TopN的商品ID ； windowEnd：每5分钟更新一次时的更新时间；  count：该商品的点击数即热度
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object hotitem_analysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //设置全局并行度为1
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)    //设置时间语义为eventTime

    val inputStream:DataStream[String] = env.readTextFile("E:\\ideaProject\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream:DataStream[UserBehavior]=inputStream
      .map(data=>{
        var arr=data.split(",")
        UserBehavior(arr(0).toLong,arr(1).toLong,arr(1).toInt,arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)    //设置哪个字段作为eventTime。assignAscendingTimestamps只可以数据依次升序使用，实际生产环境一般要改为assignTimestampsAndWatermarks见笔记waterMask实现。

    /*
    （keyby根据itemnID分组+滑动窗口）+增量聚合函数和全窗口函数 实现一个小时内各个商品累计被浏览次数
    辅助理解：
      1.全窗口函数一定是到点了关窗后才计算输出，此需求里CountAgg永远在实时执行，ItemViewWindowResult永远是5分钟执行一次。
      2.假如keyby的key有n个，且窗口大小1小时步长5分钟就同时有12个窗口（不重叠），所以永远有12*n个CountAgg同时在执行
      3.假如keyby的key有n个，不管怎样，永远间隔5分钟关窗了ItemViewWindowResult才同时运行n次（因为n个key）
     */
    val aggStream:DataStream[ItemViewCount]=dataStream
      .filter(_.behavior=="pv") // pv-浏览量
      .keyBy("itemId") //根据商品ID分组。这里注意是"itemId"而不是_.itemId。前者是字段名后者是样例类的真实值
      .timeWindow(Time.hours(1),Time.minutes(5))  //1小时范围，5分钟步长的滑动窗口
      .aggregate(new CountAgg(),new ItemViewWindowResult())//第一个增量函数 第二个全窗口函数，全窗口压力太大所以预聚合。似乎reduce也可以实现？类似wordCount？
      //实际测试出现过了5分钟但控制台没有打印，是因为窗口确实关闭了并输出了所有key的iterViewCount，只是下一步定时器那里因为延迟所以没有输出

    /*
    主要作用是排序并输出
     */
    val resultStream:DataStream[String]=aggStream
        .keyBy("windowEnd")   //这里key有一个作用：多个并行度时，keyby实现各个slot上的数据聚合
        .process(new TopNHotItems(5))   //这里又keyby又定时器：我感觉是防止之前的时间窗口数据因为网络延迟混进来，keyby+定时器之后一定能保证本次统计的就是这次时间窗口的数据。温馨提示：既要状态编程又要定时器拿出大招：processFunction。


    dataStream.print("data")
    aggStream.print("agg")
    resultStream.print()

    env.execute("hot items")
  }
}



/*定义AggregateFunction（增量聚合）函数，作用是预处理，要实现四个方法。
  UserBehavior：是输入类型
  Long：是中间计算时临时状态的类型
  Long：是输出类型，也就是输出给下一个ItemViewWindowResult自定义全窗口函数的类型
 */
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L//聚合函数累加器的初始值
  override def add(in: UserBehavior, acc: Long): Long = acc+1//每一条数据都调用一次add方法【此需求是累加器+1】
  override def getResult(acc: Long): Long = acc//要输出的，因为是增量函数，所以是实时输出（输出给下一步全窗口函数）
  override def merge(acc: Long, acc1: Long): Long = acc+acc1//merge没啥用，只在会话窗口session时用
}



/*定义WindowFunction（全窗口）函数，要实现apply方法
  Long:         输入数据类型，就是前一步aggregate函数输出的数据类型，此需求就是商品的count[Long类型]
  ItemViewCount：输出数据类型，此需求就是封装好的样例类ItemViewCount
  Tuple：      key的数据类型，我以为是商品id也就是长整型但实际keyby之后keyStream的key是flink的java tuple类型（一元组），这里要注意。
  timeWindow： 指定窗口类型是时间窗口而不是计数窗口
 */
class ItemViewWindowResult() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId=key.asInstanceOf[Tuple1[Long]].f0 //从key中取数据要注意。【此需求就是从key取出商品id】
    val windowEnd=window.getEnd   //window.getEnd就是当前窗口设定的关窗时间戳【此需求就是作为TopN更新的时间点打印展示】
    val count=input.iterator.next()  //因为input是迭代器，所以通过迭代器取数据，其实这里就一个数-count。
    out.collect(ItemViewCount(itemId,windowEnd,count)) //上面三行取出数据，封装成  ItemViewCount  输出。
  }
}



/*定义KeyedProcessFunction，重写processElement方法（每条数据都要走一遍）
  Tuple：    key的类型
  ItemViewCount：输入的类型
  String：   输出的类型
 */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  var itemViewCountListState:ListState[ItemViewCount]=_  //定义一个list型状态变量

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list",classOf[ItemViewCount]))//[ItemViewCount]都是指ListState状态量内存储的数据都是ItemViewCount类型。
  }

  /*
  一个窗口内各个不同itemID的ItemViewCount都走一遍此方法。
   */
  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemViewCountListState.add(i)//将全窗口函数输出的数据统一接收存到刚创建的状态变量中。
    context.timerService().registerEventTimeTimer(i.windowEnd+100)//当全窗函数输出的第一条数据到的时候创建定时器，之后不会重复创建。这里需要留点时间让全窗口函数后面还没输出完的数据进来，否则定时器立刻执行就几条数据来了大部队还在后面呢。
    //注意上面这里，i.windowEnd+100ms也是根据eventTime来的，即使真实世界时间一直在走它也是根据数据的evenetTime来判断的？
    //也就是说时间语义设置eventTime后，flink内所有时间都按数据源的eventTime来？？
  }

  //onTimer是定时器触发时执行的操作。当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出TopN了。主要是求TopN。
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // list状态量内数据不方便排序，移到ListBuffer中（如果java写可以是arraylist只要自己能实现排序就行）
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      allItemViewCounts += iter.next()
    }
    itemViewCountListState.clear()// 状态量中数据已经拿到了就可以清空状态量节省内存
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)// ListBuffer按照count排序，默认升序(Ordering.Long.reverse)反转后就是降序，take(topSize)取前topSize个

    /* 将排名信息格式化成String，便于打印输出可视化展示：
    窗口结束时间：2017-11-26 09:10:00.0
    NO1: 	商品ID = 812879	热门度 = 5
    NO2: 	商品ID = 2600165	热门度 = 4
    NO3: 	商品ID = 2828948	热门度 = 4
    NO4: 	商品ID = 2338453	热门度 = 4
    NO5: 	商品ID = 4261030	热门度 = 4
    ==================================
     */
    val result: StringBuilder = new StringBuilder

    result.append("窗口结束时间：").append( new Timestamp(timestamp - 100) ).append("\n")  //注意前面关窗后延迟多少毫秒这里打印时要再减去
    for( i <- sortedItemViewCounts.indices ){
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }
    result.append("\n==================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}