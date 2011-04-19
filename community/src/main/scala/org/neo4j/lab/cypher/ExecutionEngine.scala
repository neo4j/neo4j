package org.neo4j.lab.cypher

import commands._
import pipes.{FilteringPipe, Pipe, FromPump}
import org.neo4j.graphdb.{NotFoundException, Node, GraphDatabaseService}


/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {

  def createProjectionTransformers(select: Select): Seq[(Map[String, Any]) => Map[String, Any]] = {
    val transformers = select.selectItems.map((selectItem) => {
      selectItem match {
        case NodePropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
        case NodeOutput(nodeName) => nodeOutput(nodeName) _
      }
    })
    transformers
  }

  def execute(query: Query): Projection = query match {
    case Query(select, from, where) => {

      val sources: Pipe = createSourcePump(from)

      val filteredSources = where match {
        case None => sources
        case Some(w) => {
          w.clauses.head match {
            case StringEquals(variable, propName, expectedValue) => {
              new FilteringPipe(sources, (map) => {
                val value = map.getOrElse(variable, throw new NotFoundException()).asInstanceOf[Node]
                value.getProperty(propName) == expectedValue
              })
            }
          }
        }
      }

      val transformers: Seq[(Map[String, Any]) => Map[String, Any]] = createProjectionTransformers(select)


      new Projection(filteredSources, transformers)
    }
  }


  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    var node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }


  private def createSourcePump(from: scala.List[VariableAssignment]): Pipe = {
    val roots = from.map((va) => {
      val f: () => Seq[Node] = va.fromitem match {
        case NodeById(ids) => () => ids.map(graph.getNodeById)
      }

      new FromPump(va.variable, f.apply()).asInstanceOf[Pipe]
    })

    val root: Pipe = roots.reduceLeft(_.join(_))
    root
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