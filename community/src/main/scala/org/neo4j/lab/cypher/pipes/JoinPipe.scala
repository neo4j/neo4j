package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:01 
 */

class JoinPipe(a: Pipe, b: Pipe) extends Pipe {
  def foreach[U](f: (Map[String, Any]) => U) {
    a.foreach((aMap) => {
      b.foreach((bMap) => {
        f.apply(aMap ++ bMap)
      })
    })
  }
}