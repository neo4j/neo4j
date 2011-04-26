package org.neo4j.lab.cypher

import pipes.Pipe

/**
 * Created by Andres Taylor
 * Date: 4/21/11
 * Time: 15:12 
 */


class CurrentRow {
  val sources: collection.mutable.Map[String, Pipe] = new collection.mutable.HashMap[String, Pipe]

  def areDependenciesMet(pipe: Pipe): Boolean = pipe.dependsOn.forall(sources.contains(_))

  def addPipe(pipe: Pipe): Traversable[Pipe] = areDependenciesMet(pipe) match {
    case false => List(pipe)
    case true => {

      if (pipe.dependsOn.nonEmpty) {
        val input = getPipeFor(pipe.dependsOn)
        pipe.setInput(input)
      }

      pipe.columnNames.foreach((column) => sources += (column -> pipe))

      List()
    }
  }

  def getPipeFor(dependencies: List[String]): Pipe = {
    val result = sources.
      filter((kv) => dependencies.contains(kv._1)).
      values.
      toList.
      distinct.
      reduceLeft(_ ++ _)

    result.columnNames.foreach(sources.remove)

    result
  }


  def getPipeForColumns(neededColumns: List[String]): Option[Pipe] = {
    var pipes = List[Pipe]()
    var leftToDo = neededColumns

    while (leftToDo.nonEmpty) {
      val pipe = sources.get(leftToDo.head) match {
        case None => return None
        case Some(x) => {
          x.columnNames.foreach(sources.remove(_))
          leftToDo = leftToDo.filterNot(x.columnNames.contains(_))
          x
        }
      }
      pipes = pipes ++ List(pipe)
    }

    Some(pipes.reduceLeft(_ ++ _))
  }

  def constructPipe(): Pipe = sources.values.toList.distinct.reduceLeft(_ ++ _)

}