package org.neo4j.lab.cypher

import java.lang.Exception

/**
 * Created by Andres Taylor
 * Date: 5/20/11
 * Time: 14:08 
 */

class SyntaxError(message:String, cause:Throwable) extends Exception(message, cause) {
  def this(message:String) = this(message, null)
}