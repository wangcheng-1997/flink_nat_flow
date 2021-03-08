import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object NatFlow {

  case class NATBean(accesstime: Long, hostIP: String, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String, username: String, rowkey: String)

  case class NATLog(accesstime: Long, hostIP: String, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String)

  case class NATAnalyze(accesstime: Long, hostIP: String, sourceIp: String, sourcePort: String, targetIp: String, targetPort: String, protocol: String, convertedIp: String, convertedPort: String, operator: String, province: String, city: String)

  val httpHosts = new java.util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("30.250.60.21", 9200, "http"))
  httpHosts.add(new HttpHost("30.250.60.22", 9200, "http"))
  httpHosts.add(new HttpHost("30.250.60.23", 9200, "http"))

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(6)
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "30.250.11.139:9092,30.250.11.140:9092,30.250.11.141:9092")
    props.setProperty("enable.auto.commit", "true")
    props.setProperty("auto.commit.interval.ms", "1000")
    props.setProperty("max.partition.fetch.bytes", "24") // 设置分区消费消息最大大小
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val url1 = "jdbc:mysql://30.250.11.142:3306/broadband?characterEncoding=utf-8&useSSL=false"
    val url2 = "jdbc:mysql://30.250.11.142:3306/nat_log?characterEncoding=utf-8&useSSL=false"
    val userName1 = "admin"
    val password1 = "SX_enable!@#123"

    //注册Driver
    Class.forName("com.mysql.jdbc.Driver")

    // kafka数据消费创建数据流
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010("syslog", new SimpleStringSchema(), props)
    kafkaConsumer.setStartFromLatest()

    val baseStream: DataStream[NATLog] = env.addSource(kafkaConsumer)
      .map(_.split(","))
      .filter(_.length >= 2)
      .map(per => {
        val strings1: Array[String] = per(0).split("\\s+")
        val strings2: Array[String] = per(1).split(" snat to ")
        val str = strings1(0).replaceAll("T", " ")
        val date =
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
            str.substring(0, 14) + str.substring(14, 16).toInt / 5 * 5 + ":00"
          )
            .getTime
        val hostIP = strings1(1)
        val corresponds = strings1(6).split("[->()]")
        val sourceIpPort = corresponds(0)
        val targetIpPort = corresponds(2)
        val convertedIpPort = strings2(1)
        var protocol = ""
        if (corresponds(3).endsWith(":")) {
          protocol = corresponds(3).substring(0, corresponds(3).length - 1)
        } else {
          protocol = corresponds(3)
        }
        val sourceIp = sourceIpPort.split(":")(0)
        val sourcePort = sourceIpPort.split(":")(1)
        val targetIp = targetIpPort.split(":")(0)
        val targetPort = targetIpPort.split(":")(1)
        val convertedIp = convertedIpPort.split(":")(0)
        val convertedPort = convertedIpPort.split(":")(1)

        NATLog(date, hostIP, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort)
      })


    val LogStream = baseStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[NATLog]
      (Time.seconds(0)) {
        override def extractTimestamp(element: NATLog): Long = {
          element.accesstime
        }
      }
    ).keyBy(_.accesstime)
      .timeWindow(Time.seconds(300))

    // TODO 数据源日志计数
    LogStream.process(new ProcessWindowFunction[NATLog, (Long, Int), Long, TimeWindow] {
      override def process(key: Long, context: Context, elements: Iterable[NATLog], out: Collector[(Long, Int)]): Unit = {
        val count = elements.size
        val count_connection = DriverManager.getConnection(url2, userName1, password1)
        val statement = count_connection.createStatement
        statement.executeUpdate(s"insert into nat_count (count_5min,count_sec,update_time) values ('$count','${count / 300}','${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(key)}')")
        count_connection.close()
        out.collect((key, count))
      }
    })

    // TODO 1 日志记录流
    val HbaseStream = LogStream.process(new ProcessWindowFunction[NATLog, NATBean, Long, TimeWindow] {
      override def process(key: Long, context: Context, elements: Iterable[NATLog], out: Collector[NATBean]): Unit = {
        val username_connection = DriverManager.getConnection(url1, userName1, password1)
        val statement1 = username_connection.createStatement

        var usernames = Map[String, String]()
        val name = statement1.executeQuery(s"""select framedip,loginname from aaa_bb_online""")
        while (name.next) {
          usernames ++= Map(name.getString("framedip") -> name.getString("loginname"))
        }
        username_connection.close()

        for (i <- elements) {
          val username = usernames.getOrElse(i.sourceIp, "UnKnown")
          val rowkey = MyUtils.MD5Encode(i.sourceIp + i.sourcePort + i.targetIp + i.targetPort + i.convertedIp + i.convertedPort).substring(8, 24) + "_" + i.accesstime
          out.collect(NATBean(i.accesstime, i.hostIP, i.sourceIp, i.sourcePort, i.targetIp, i.targetPort, i.protocol, i.convertedIp, i.convertedPort, username, rowkey))
        }

      }
    }).filter(_.username != "UnKnown")

    // TODO 1.1 用户统计
    HbaseStream.map(per => (per.username, per.accesstime))
      .addSink(new UsernameHDFSSink)

    // TODO 1.2 日志计数
    HbaseStream.map(per => (per.accesstime, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(300))
      .process(new ProcessWindowFunction[(Long, Int), (Long, Int), Tuple, TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(Long, Int)], out: Collector[(Long, Int)]): Unit = {
          val count = elements.size
          val count_connection = DriverManager.getConnection(url2, userName1, password1)
          val statement = count_connection.createStatement
          val start = context.window.getStart
          statement.executeUpdate(s"insert into nat_hbase_count (count_5min,count_sec,update_time) values ('$count','${count / 300}','${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(start)}')")
          count_connection.close()
          out.collect((start, count))
        }
      })

    // TODO 1.3 ES实现二级索引
    val esSink: ElasticsearchSink.Builder[util.HashMap[String, String]] = new ElasticsearchSink.Builder[util.HashMap[String, String]](httpHosts, new ElasticsearchSinkFunction[util.HashMap[String, String]] {
      override def process(t: util.HashMap[String, String], runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        def createRequest(element: util.HashMap[String, String]): IndexRequest = {
          Requests.indexRequest(s"bigdata_nat_hbase_test_0").`type`("hbase").id(element.get("rowkey"))
            .source(element)
        }

        requestIndexer.add(createRequest(t))
      }
    })
    esSink.setBulkFlushMaxActions(1)
    HbaseStream.map(per => {
      val json = new java.util.HashMap[String, String]
      json.put("sourceIp", per.sourceIp)
      json.put("sourcePort", per.sourcePort)
      json.put("targetIp", per.targetIp)
      json.put("targetPort", per.targetPort)
      json.put("convertedIp", per.convertedIp)
      json.put("convertedPort", per.convertedPort)
      json.put("accesstime", per.accesstime.toString)
      json.put("rowkey", per.rowkey)
      json
    }).addSink(esSink.build())

    // TODO 1.4 日志记录sink到Hbase
    HbaseStream.addSink(new HBaseWrite)

    // TODO 2 日志分析流
    NATAnalyze(baseStream)

    // 运行环境执行
    env.execute("NATFlow")

  }

  // 日志分析(ip分析写入hdfs未完成)
  def NATAnalyze(baseStream: DataStream[NATLog]): DataStreamSink[util.HashMap[String, String]] = {
    val analyzeStream: DataStream[NATAnalyze] = baseStream.map(per => {
      // 获取运营商
      var operator = "UnKnown"
      //获取省份
      var province = "UnKnown"
      //获取城市
      var city = "UnKnown"

      val maps = myUtil.IpSearch.getRegionByIp(per.targetIp)

      if (!maps.isEmpty) {
        operator = maps.get("运营").toString
        province = maps.get("省份").toString
        city = maps.get("城市").toString
      }

      NATAnalyze(per.accesstime, per.hostIP, per.sourceIp, per.sourcePort, per.targetIp, per.targetPort, per.protocol, per.convertedIp, per.convertedPort, operator, province, city)
    }).filter(per => {
      (per.operator != "Unknown" || per.province != "Unknown" || per.city != "Unknown") && (per.operator != "" || per.province != "" || per.city != "")
    })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[NATAnalyze]
        (Time.seconds(0)) {
          override def extractTimestamp(element: NATAnalyze): Long = {
            element.accesstime
          }
        }
      )

    // 数据分析流新建自定义ES Sink
    val esSink: ElasticsearchSink.Builder[util.HashMap[String, String]] = new ElasticsearchSink.Builder[util.HashMap[String, String]](httpHosts, new ElasticsearchSinkFunction[util.HashMap[String, String]] {
      override def process(t: util.HashMap[String, String], runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        def createRequest(element: util.HashMap[String, String]): IndexRequest = {
          Requests.indexRequest().index(s"bigdata_nat_flow_test_0")
            .`type`("nat")
            .source(element)
        }

        requestIndexer.add(createRequest(t))
      }
    })
    esSink.setBulkFlushMaxActions(1)

    // TODO 2.1 省份维度聚合
    val province: DataStream[util.HashMap[String, String]] = analyzeStream.map(per => (per.province, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(300))
      .process(new MyProcessWindowFunction)
      .map(per => {
        tuple2Json(per, "province")
      })
        .process(new ProcessFunction(){
          override def processElement(value: util.HashMap[String, String], ctx: ProcessFunction[util.HashMap[String, String], util.HashMap[String, String]]#Context, out: Collector[util.HashMap[String, String]]): Unit = {

          }
        })

    province.addSink(esSink.build())

    // TODO 2.2 运营商维度聚合
    val operator: DataStream[util.HashMap[String, String]] = analyzeStream.map(per => (per.operator, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(300))
      .process(new MyProcessWindowFunction)
      .map(per => {
        tuple2Json(per, "operator")
      })

    operator.addSink(esSink.build())

    // TODO 2.3 目标IP维度聚合(sink hdfs 未完成)
    //        analyzeStream.map(per => {
    //          ((per.accesstime, per.targetIp), 1)
    //        }).keyBy(0).sum(1)
    //          .filter(_._2 > 10).map(per => (per._1._1, per._1._2, per._2))

    // TODO 2.4 省会维度聚合
    val city: DataStream[util.HashMap[String, String]] = analyzeStream.map(per => (per.city, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(300))
      .process(new MyProcessWindowFunction)
      .map(per => {
        val types = "city"
        tuple2Json(per, types)
      })

    city.addSink(esSink.build())
  }

  // 自定义HDFS Sink
  class UsernameHDFSSink extends RichSinkFunction[(String, Long)] {
    var conf: org.apache.hadoop.conf.Configuration = _
    var fs: FileSystem = _
    val df = new SimpleDateFormat("yyyyMMddHHmm")

    override def open(parameters: Configuration): Unit = {
      conf = new org.apache.hadoop.conf.Configuration()
      conf.set("fs.defaultFS", "hdfs://nns")
      conf.set("dfs.nameservices", "nns")
      conf.set("dfs.ha.namenodes.nns", "nn1,nn2")
      conf.set("dfs.namenode.rpc-address.nns.nn1", "30.250.60.2:8020")
      conf.set("dfs.namenode.rpc-address.nns.nn2", "30.250.60.7:8020")
      conf.set("dfs.client.failover.proxy.provider.nns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
      conf.setBoolean("dfs.support.append", true)

      fs = FileSystem.get(conf)
    }

    override def invoke(value: (String, Long), context: SinkFunction.Context[_]): Unit = {
      val date = df.format(value._2)
      val path = new Path(s"hdfs://nns/nat_test/username/${date.substring(0, 8)}/username.txt")
      if (!fs.exists(path)) {
        fs.createNewFile(path)
      }
      val append = fs.append(path)
      append.write(value.toString().getBytes("UTF-8"))
      append.write(10)
      append.flush()
      append.close()
    }

    override def close(): Unit = {
      if (fs != null) {
        try {
          fs.close()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
  }

  // 元组转json
  def tuple2Json(per: (String, Long, Long), types: String): util.HashMap[String, String] = {
    val json = new java.util.HashMap[String, String]
    json.put("types", types)
    json.put("data", per._1)
    json.put("count", per._2.toString)
    json.put("accesstime", per._3.toString)
//    json.put("username",per.toString())
    json
  }

  // 自定义Hbase Sink
  class HBaseWrite extends RichSinkFunction[NATBean] {
    var conn: Connection = _
    var mutator: BufferedMutator = _
    var count = 0

    override def open(parameters: Configuration): Unit = {
      val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
      config.set(HConstants.ZOOKEEPER_QUORUM, "30.250.60.2,30.250.60.3,30.250.60.5,30.250.60.6,30.250.60.7")
      config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
      config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
      conn = ConnectionFactory.createConnection(config)

      val tableName: TableName = TableName.valueOf("nat:test")
      val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
      //设置缓存1m，当达到1m时数据会自动刷到hbase
      params.writeBufferSize(1024 * 512) //设置缓存的大小
      mutator = conn.getBufferedMutator(params)
      count = 0
    }

    override def invoke(value: NATBean, context: SinkFunction.Context[_]): Unit = {

      val rowkey: Array[Byte] = Bytes.toBytes(value.rowkey)

      val put = new Put(rowkey)
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("accesstime"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value.accesstime)))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("hostIP"), Bytes.toBytes(value.hostIP))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourceIp"), Bytes.toBytes(value.sourceIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("sourcePort"), Bytes.toBytes(value.sourcePort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetIp"), Bytes.toBytes(value.targetIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("targetPort"), Bytes.toBytes(value.targetPort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("protocol"), Bytes.toBytes(value.protocol))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedIp"), Bytes.toBytes(value.convertedIp))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("convertedPort"), Bytes.toBytes(value.convertedPort))
      put.addColumn(Bytes.toBytes("nat"), Bytes.toBytes("username"), Bytes.toBytes(value.username))

      mutator.mutate(put)
      //每满2000条刷新一下数据
      if (count >= 1000) {
        mutator.flush()
        count = 0
      }
      count = count + 1
    }

    override def close(): Unit = {
      if (conn != null) conn.close()
    }
  }

  // 日志分析自定义窗口进程
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), (String, Long, Long), Tuple, TimeWindow] {
    override def process(key: Tuple,
                         context: Context,
                         elements: Iterable[(String, Int)],
                         out: Collector[(String, Long, Long)]): Unit = {
      var sum = 0
      for (in <- elements) {
        sum += in._2
      }
      val time = context.window.getStart / 1000
      out.collect((key.getField(0), sum, time))
    }
  }

}