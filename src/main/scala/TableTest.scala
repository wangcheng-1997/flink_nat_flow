//import java.sql.DriverManager
//import java.text.SimpleDateFormat
//import java.util.Properties
//
//import NatFlow.NATBean
//import org.apache.flink.api.scala._
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.util.Collector
//
//object TableTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//
//    val tenv = StreamTableEnvironment.create(env)
//
//    //    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//    val props = new Properties()
//    props.setProperty("bootstrap.servers", "30.250.11.139:9092,30.250.11.140:9092,30.250.11.141:9092")
//    props.setProperty("enable.auto.commit", "true")
//    props.setProperty("auto.commit.interval.ms", "1000")
//    //    props.setProperty("max.partition.fetch.bytes", "24") // 设置分区消费消息最大size 8M
//    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty("flink.partition-discovery.interval-millis", "1000")
//
//
//    val url = "jdbc:mysql://30.250.11.142:3306/broadband?characterEncoding=utf-8&useSSL=false"
//
//    val userName1 = "admin"
//    val password1 = "SX_enable!@#123"
//
//    //注册Driver
//    Class.forName("com.mysql.jdbc.Driver")
//
//    //得到连接
//    val username_connection = DriverManager.getConnection(url, userName1, password1)
//    val statement1 = username_connection.createStatement
//    var usernames = Map[String, String]()
//    val name = statement1.executeQuery(s"""select framedip,loginname from aaa_bb_online""")
//    while (name.next) {
//      usernames ++= Map(name.getString("framedip") -> name.getString("loginname"))
//    }
//
//
//    val value: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010("syslog", new SimpleStringSchema(), props)
//    value.setStartFromLatest()
//    val value1: DataStream[NATBean] = env.addSource(value)
//      .map(_.split(","))
//      .filter(_.length >= 2)
//      .map(per => {
//        val strings1: Array[String] = per(0).split("\\s+")
//        val strings2: Array[String] = per(1).split(" snat to ")
//        val str = strings1(0).replaceAll("T", " ")
//        val date =
//          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
//            str.substring(0, 14) + str.substring(14, 16).toInt
//              //              / 5 * 5
//              + ":00"
//          )
//            .getTime
//        //        / 1000
//        val hostIP = strings1(1)
//        val corresponds = strings1(6).split("[->()]")
//        val sourceIpPort = corresponds(0)
//        val targetIpPort = corresponds(2)
//        val convertedIpPort = strings2(1)
//        var protocol = ""
//        if (corresponds(3).endsWith(":")) {
//          protocol = corresponds(3).substring(0, corresponds(3).length - 1)
//        } else {
//          protocol = corresponds(3)
//        }
//        val sourceIp = sourceIpPort.split(":")(0)
//        val sourcePort = sourceIpPort.split(":")(1)
//        val targetIp = targetIpPort.split(":")(0)
//        val targetPort = targetIpPort.split(":")(1)
//        val convertedIp = convertedIpPort.split(":")(0)
//        val convertedPort = convertedIpPort.split(":")(1)
//
//        // 获取运营商
//        var operate = "UnKnown"
//        //获取省份
//        var province = "UnKnown"
//        //获取城市
//        var city = "UnKnown"
//
//        val maps = myUtil.IpSearch.getRegionByIp(targetIp)
//
//        if (!maps.isEmpty) {
//          operate = maps.get("运营").toString
//          province = maps.get("省份").toString
//          city = maps.get("城市").toString
//        }
//
//        val username = usernames.getOrElse(sourceIp, "UnKnown")
//
//        val rowkey = MyUtils.MD5Encode(sourceIp + sourcePort + targetIp + targetPort + convertedIp + convertedPort).substring(8, 24) + "_" + date
//
//        NATBean(date, hostIP, sourceIp, sourcePort, targetIp, targetPort, protocol, convertedIp, convertedPort, operate, province, city, username, rowkey)
//      })
//      .filter(_.province != "")
//      .filter(_.province != "UnKnown")
//      .filter(_.username != "UnKnown")
//
//    val tb = tenv.fromDataStream(value1)
//    tenv.registerTable("nat_flow",tb)
//
//    val table: Table = tenv.sqlQuery(
//      """
//        |select
//        |province,count(1) resolver
//        |from
//        |nat_flow
//        |group by province
////        |order by resolver desc
//        |""".stripMargin)
//
//    val value2: DataStream[(Boolean, result)] = tenv.toRetractStream[result](table)
//    value2.map(per => (per._2.province,per._2.resolver)).keyBy(0).timeWindow(Time.seconds(5))
//      .max(1)
//      .print()
//
//    env.execute()
//
//  }
//
//  case class result(province:String,resolver:Long)
//
//}
