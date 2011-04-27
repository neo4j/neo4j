package org.neo4j.lab.cypher

import commands._
import org.neo4j.graphdb.{NotFoundException, Node, GraphDatabaseService}
import pipes.{RelatedToPipe, FilteringPipe, Pipe, FromPump}


/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  type MapTransformer = (Map[String, Any]) => Map[String, Any]

  def makeMutable(immutableSources: Map[String, Pipe]): collection.mutable.Map[String, Pipe] = scala.collection.mutable.Map(immutableSources.toSeq: _*)

  def execute(query: Query): Projection = query match {
    case Query(select, from, where) => {

      var sourcePumps: List[Pipe] = createSourcePumps(from)
      val filters: Option[Pipe] = createFilters(where)

      val currentRow = new CurrentRow()
      while (sourcePumps.nonEmpty) {
        sourcePumps = sourcePumps.flatten(currentRow.addPipe)
      }



      if (filters.nonEmpty) {
        currentRow.addPipe(filters.get)
      }

      //      sourcePumps.foreach(currentRow.addPipe)

      val transformers: Seq[MapTransformer] = createProjectionTransformers(select)

      //      val source = sourcePumps.reduceLeft(_ ++ _)

      new Projection(currentRow.constructPipe(), transformers)
    }
  }


  def createProjectionTransformers(select: Select): Seq[MapTransformer] = select.selectItems.map((selectItem) => {
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
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }


  private def createSourcePumps(from: List[VariableAssignment]): List[Pipe] = from.map((va) => va.fromitem match {
    case NodeById(ids) => new FromPump(va.variable, ids.map(graph.getNodeById))
    case RelatedTo(column, relType, direction) => new RelatedToPipe(column, va.variable, relType, direction)
    //        case NodeByIndex(idx, value) => graph.index.forNodes("idx").get()
  })


  private def createSelectOutput[T](select: Select, inputNodes: scala.Seq[Node]): List[T] = {
    val selectItem = select.selectItems.head

    val transformer: (Node) => T = selectItem match {
      case NodeOutput(variable) => (n) => n.asInstanceOf[T]
      case NodePropertyOutput(nodeName, propName) => (n) => n.getProperty(propName).asInstanceOf[T]
    }

    inputNodes.map(transformer).toList
  }
}