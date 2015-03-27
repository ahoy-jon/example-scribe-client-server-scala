package scribeserver

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.{lang, util}


import org.apache.thrift.transport.{TFramedTransport, TSocket}
import scribe.thrift.scribe.Processor
import scribe.thrift.{scribe, LogEntry, ResultCode}


import java.{lang, util}
import com.facebook.fb303.fb_status
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.THsHaServer
import org.apache.thrift.transport.TNonblockingServerSocket





case class ScribeServer(port:Int, handler: Seq[LogEntry] => Boolean) {

  class ScribeServerHandler extends scribe.Iface {

    override def Log(messages: util.List[LogEntry]): ResultCode = {
      import scala.collection.JavaConversions._
      if(handler(messages.toList)) {
        ResultCode.OK
      } else {
        ResultCode.TRY_LATER
      }

    }

    override def shutdown(): Unit = {}

    override def getOptions: util.Map[String, String] = new util.HashMap[String, String]

    override def getCounter(key: String): Long = 0

    override def reinitialize(): Unit = {}

    override def getName: String = "myScribeServer"

    override def setOption(key: String, value: String): Unit = {}

    override def getCounters: util.Map[String, lang.Long] = new util.HashMap[String, lang.Long]

    override def getStatusDetails: String = "n/a"

    override def getStatus: fb_status = fb_status.ALIVE

    override def getOption(key: String): String = "n/a"

    override def getVersion: String = "0"

    override def getCpuProfile(profileDurationInSec: Int): String = "n/a"

    override def aliveSince(): Long = 0
  }



  lazy val thread: Thread = new Thread(new Runnable() {
    override def run() {

      val processor = new Processor(new ScribeServerHandler)

      val args = new THsHaServer.Args(new TNonblockingServerSocket(port)).processor(processor).protocolFactory(new TBinaryProtocol.Factory()).workerThreads(1)

      val server = new THsHaServer(args)

      println(s"----------------Starting the scribe server on port $port .--------------------------")
      server.serve()

    }
  })

  def start():Unit = {
    thread.setDaemon(true)
    thread.start()
  }
}






object Client extends App {


  val server =  ScribeServer(8383, x => {x.foreach(println) ;  true})


  def send() {
    val socket: Socket = new Socket
    socket.connect(new InetSocketAddress("localhost", 8383), 1000)
    val sock: TSocket = new TSocket(socket)
    val transport: TFramedTransport = new TFramedTransport(sock)
    val protocol: TBinaryProtocol = new TBinaryProtocol(transport, false, false)
    val client = new scribe.Client(protocol, protocol)


    println("connected")

    while (true ) {
      val resultCode: ResultCode = client.Log(util.Arrays.asList(new LogEntry("test", new util.Date().toString)))
      Thread.sleep(1000);

    }
  }


  server.start()

  Thread.sleep(1000) // should go for server.isServing

  send()

}
