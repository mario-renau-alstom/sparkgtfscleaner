package com.alstom.tools

import org.apache.log4j.Logger
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *  Add new type of Logging output
 */
object Implicits {
    
    implicit class LogExtender( logger: org.apache.log4j.Logger ) {
      def file( msg: String ) = logger.log(FileLog4jLevel.FILE, msg)
    }
  
}

/**
 *  Implementation of extra methods for pretty printing of logs
 */
object logAssistance {
  
  def getCaseAccessors[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

  def nice[T:TypeTag](x: T)(implicit classTag: ClassTag[T]) : String = {
    val instance = x.asInstanceOf[T]
    val mirror = runtimeMirror(instance.getClass.getClassLoader)
    val accessors = getCaseAccessors[T]
    var res = List.empty[String]
    accessors.foreach { z =>
      val instanceMirror = mirror.reflect(instance)
      val fieldMirror = instanceMirror.reflectField(z.asTerm)
      val s = s"${z.name} = ${fieldMirror.get}"
      res = s :: res
    }
    val beautified = x.getClass.getSimpleName + "(" + res.mkString(", ") + ")"
    beautified
  }
  
}