package debug.flink.state.example

import java.io.File

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.reflect.io
import scala.reflect.io.Directory

/**
 * 实时计算事件总个数，以及value总和
 *
 * @author shirukai
 */

object EventCounterJob {

  def main(args: Array[String]): Unit = {

    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    var params: ParameterTool = ParameterTool.fromArgs(args)
    val checkPointDirPath = params.get("checkpoint-dir")
    // 获取最后一个checkpoint文件夹
    val checkpointDirs = new io.Directory(new File(checkPointDirPath)).list
    if (checkpointDirs.nonEmpty) {
      val lastCheckpointDir = checkpointDirs.maxBy(_.lastModified)
      val checkpoints = new Directory(lastCheckpointDir.jfile).list.filter(_.name.startsWith("chk-"))
      if (checkpoints.nonEmpty) {
        val lastCheckpoint = checkpoints.maxBy(_.lastModified).path
        val newArgs = Array("--last-checkpoint", "file://" + lastCheckpoint)
        // 重新载入配置
        params = ParameterTool.fromArgs(args ++ newArgs)
      }
    }
    env.getConfig.setGlobalJobParameters(params)

    // 配置checkpoint
    // 做两个checkpoint的间隔为1秒
    env.enableCheckpointing(1000)
    // 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置状态后端：MemoryStateBackend、FsStateBackend、RocksDBStateBackend，这里设置基于文件的状态后端
    env.setStateBackend(new FsStateBackend("file://"+checkPointDirPath))


    // 1. 从socket中接收文本数据
    val streamText: DataStream[String] = env.socketTextStream("127.0.0.1", 9000)

    // 2. 将文本内容按照空格分割转换为事件样例类
    val events = streamText.map(s => {
      val tokens = s.split(" ")
      Event(tokens(0), tokens(1).toDouble, tokens(2).toLong)
    })
    // 3. 按照时间id分区，然后进行聚合统计
    val counterResult = events.keyBy(_.id)
      .process(new EventCounterProcessFunction)

    // 4. 结果输出到控制台
    counterResult.print()

    env.execute("EventCounterJob")
  }
}

/**
 * 定义事件样例类
 *
 * @param id    事件类型id
 * @param value 事件值
 * @param time  事件时间
 */
case class Event(id: String, value: Double, time: Long)

/**
 * 定义事件统计器样例类
 *
 * @param id    事件类型id
 * @param sum   事件值总和
 * @param count 事件个数
 */
case class EventCounter(id: String, var sum: Double, var count: Int)

/**
 * 继承KeyedProcessFunction实现事件统计
 */
class EventCounterProcessFunction extends KeyedProcessFunction[String, Event, EventCounter] {
  private var counterState: ValueState[EventCounter] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 从flink上下文中获取状态
    counterState = getRuntimeContext.getState(new ValueStateDescriptor[EventCounter]("event-counter", classOf[EventCounter]))
  }

  override def processElement(i: Event,
                              context: KeyedProcessFunction[String, Event, EventCounter]#Context,
                              collector: Collector[EventCounter]): Unit = {

    // 从状态中获取统计器，如果统计器不存在给定一个初始值
    val counter = Option(counterState.value()).getOrElse(EventCounter(i.id, 0.0, 0))

    // 统计聚合
    counter.count += 1
    counter.sum += i.value

    // 发送结果到下游
    collector.collect(counter)

    // 保存状态
    counterState.update(counter)

  }
}