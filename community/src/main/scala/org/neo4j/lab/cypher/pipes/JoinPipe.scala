package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:01 
 */

class JoinPipe(a: Pipe, b: Pipe) extends Pipe {

  def columnNames: List[String] = a.columnNames ++ b.columnNames

  override def setInput(input: Pipe) {
    throw new RuntimeException("Oh when will the foolishness stop? I know who to input from...!")
  }

  def dependsOn = throw new RuntimeException("A JoinPipe should not be treated like the rest of the bunch")
  def foreach[U](f: (Map[String, Any]) => U) {
    a.foreach((aMap) => {
      b.foreach((bMap) => {
        f.apply(aMap ++ bMap)
      })
    })
  }
}