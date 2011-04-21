package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

abstract class Pipe extends Traversable[Map[String, Any]] {
  var input: Option[Pipe] = None

  protected def getInput: Pipe = input match {
    case None => throw new RuntimeException("No input defined yet")
    case Some(x) => x
  }

  def dependsOn: List[String]

  def ++(other: Pipe): Pipe = new JoinPipe(this, other)

  def setInput(pipe: Pipe) {
    input = Some(pipe)
  }

  def columnNames: List[String]
}