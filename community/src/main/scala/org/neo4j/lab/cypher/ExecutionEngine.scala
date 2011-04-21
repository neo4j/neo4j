package org.neo4j.lab.cypher

import commands._
import pipes.{FilteringPipe, Pipe, FromPump}
import org.neo4j.graphdb.{NotFoundException, Node, GraphDatabaseService}

//type MapTransformer = (Map[String, Any]) => Map[String, Any]

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  def makeMutable(immutableSources: Map[String, Pipe]): collection.mutable.Map[String, Pipe] = scala.collection.mutable.Map(immutableSources.toSeq: _*)

  def execute(query: Query): Projection = query match {
    case Query(select, from, where) => {

      val sourcePumps: List[Pipe] = createSourcePumps(from)
      val currentRow = new CurrentRow(sourcePumps)


      val filters: Option[Pipe] = createFilters(where)
      val transformers: Seq[(Map[String, Any]) => Map[String, Any]] = createProjectionTransformers(select)

      val source = sourcePumps.reduceLeft(_ ++ _)

      new Projection(source, transformers)
    }
  }

  class CurrentRow(pipes: List[Pipe]) {
    val immutableSources: Map[String, Pipe] = pipes.flatMap((pipe) => pipe.columnNames.map((columnName) => Map(columnName -> pipe))).reduceLeft(_ ++ _)
    val sources: collection.mutable.Map[String, Pipe] = makeMutable(immutableSources)

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
  }

  def createProjectionTransformers(select: Select): Seq[(Map[String, Any]) => Map[String, Any]] = select.selectItems.map((selectItem) => {
    selectItem match {
      case NodePropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
      case NodeOutput(nodeName) => nodeOutput(nodeName) _
    }
  })

  def createFilters(where: Option[Where]): Option[Pipe] = {
    val filteredSources: Option[Pipe] = where match {
      case None => None
      case Some(w) => {
        w.clauses.head match {
          case StringEquals(variable, propName, expectedValue) => {
            Some(new FilteringPipe(List(variable), (map) => {
              val value = map.getOrElse(variable, throw new NotFoundException()).asInstanceOf[Node]
              value.getProperty(propName) == expectedValue
            }))

          }
        }
      }
    }
    filteredSources
  }

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    var node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }


  private def createSourcePumps(from: List[VariableAssignment]): List[Pipe] = {
    val roots = from.map((va) => {
      val f: () => Seq[Node] = va.fromitem match {
        case NodeById(ids) => () => ids.map(graph.getNodeById)
        //        case RelatedTo(column, relType, direction) =
        //        case NodeByIndex(idx, value) => graph.index.forNodes("idx").get()
      }

      new FromPump(va.variable, f.apply()).asInstanceOf[Pipe]
    })

    roots
  }


  private def createSelectOutput[T](select: Select, inputNodes: scala.Seq[Node]): List[T] = {
    val selectItem = select.selectItems.head

    val transformer: (Node) => T = selectItem match {
      case NodeOutput(variable) => (n) => n.asInstanceOf[T]
      case NodePropertyOutput(nodeName, propName) => (n) => n.getProperty(propName).asInstanceOf[T]
    }

    inputNodes.map(transformer).toList
  }
}