import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
网站累计PV统计
*/
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      .map( new MyMapper() )  // 随机key实现keyby能slot并行执行
      .keyBy( _._1 )
      .timeWindow( Time.hours(1) )    // 1小时滚动窗口
      .aggregate( new PvCountAgg(), new PvCountWindowResult() )   //和热门商品TopN类似，一个预聚合一个全窗口函数

    val totalPvStream = pvStream
      .keyBy(_.windowEnd)   //这里的keyby很重要，因为slot多并行度，keyby后只有一个slot执行汇总，这是我们想要的
      .process( new TotalPvCountResult() )

    totalPvStream.print()

    env.execute("pv job")

  }
}



// map udf，用来随机生成分组key以实现slot并行执行
class MyMapper() extends MapFunction[UserBehavior, (String, Long)]{
  override def map(value: UserBehavior): (String, Long) = {
    ( Random.nextString(10), 1L )
  }
}


//预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


//全窗口函数
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}


class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount]{
  // 单值状态，保存所有count总和
  lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    // 从状态量取出值再update回去
     val currentTotalCount = totalPvCountResultState.value()
    totalPvCountResultState.update( currentTotalCount + value.count )
    // 注册定时器，即将准备输出
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    //从最终状态量取出数据，封装样例类并输出，清空状态量
    val totalPvCount = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    totalPvCountResultState.clear()
  }
}