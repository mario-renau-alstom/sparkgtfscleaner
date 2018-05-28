package com.alstom.tools

import org.apache.log4j.Logger
import org.apache.log4j.{Appender, FileAppender, ConsoleAppender}
import org.apache.log4j.PatternLayout
import org.apache.log4j.Level
import org.apache.log4j.spi.LoggingEvent
import scala.collection.JavaConversions._

/**
 *  Configuration of Log system
 */
object Logs extends Serializable{
  
  def config_logger(appName: String, logPath: String, logLevel: Level) =
  {
    val logger = Logger.getLogger(appName)   
    
    val PATTERN = "[%X{appName}] %d{yyyy-MM-dd HH:mm:ss} %-5p - %m%n"
    
    val fa = new MyFileAppender();
    fa.setName("FileLogger");
    fa.setFile(logPath);
    fa.setLayout(new PatternLayout(PATTERN));
    fa.setThreshold(logLevel);
    fa.setAppend(true);
    fa.activateOptions();
    logger.addAppender(fa)
    
    logger.info( "Log initialized to " + logPath  )
    
  }

}

/**
 *  Specific FileAppender used for the Log system
 */
class MyFileAppender extends FileAppender {

    override def append(e: LoggingEvent) {     
      try {
        val msg = e.getMessage.asInstanceOf[String].replaceAll("\\x1b\\[[0-9;]*m", "")
        
        val myEvent = new LoggingEvent(e.fqnOfCategoryClass, e.getLogger, e.timeStamp, e.getLevel, 
            msg, e.getThreadName, e.getThrowableInformation, e.getNDC, e.getLocationInformation, e.getProperties)   
        
        super.append( myEvent )
      } catch {
        case e: Exception => e.printStackTrace(); // Dont log it as it will lead to infinite loop. Simply print the trace
      }
    }
}

