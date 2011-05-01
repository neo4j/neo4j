package org.neo4j.lab.cypher

import commands._
import pipes.{RelatedToPipe, FilteringPipe, Pipe, FromPump}
import scala.collection.JavaConverters._
import org.neo4j.graphdb.{PropertyContainer, NotFoundException, Node, GraphDatabaseService}


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

      var sourcePumps = createSourcePumps(from)
      val filters = createFilters(where)

      val currentRow = new CurrentRow()
      while (sourcePumps.nonEmpty) {
        sourcePumps = sourcePumps.flatten(currentRow.addPipe)
      }



      if (filters.nonEmpty) {
        val pipe: Traversable[Pipe] = currentRow.addPipe(filters.get)
        if (pipe.nonEmpty) {
          throw new RuntimeException("What?")
        }
      }

      val transformers: Seq[MapTransformer] = createProjectionTransformers(select)

      val constructPipe: Pipe = currentRow.constructPipe()

      new Projection(constructPipe, transformers)
    }
  }


  def createProjectionTransformers(select: Select): Seq[MapTransformer] = select.selectItems.map((selectItem) => {
    selectItem match {
      case NodePropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
      case NodeOutput(nodeName) => nodeOutput(nodeName) _
    }
  })

  def createFilters(where: Option[Where]): Option[Pipe] = {
    where match {
      case None => None
      case Some(w) => {
        w.clauses.head match {
          case StringEquals(variable, propName, expectedValue) => {
            Some(new FilteringPipe(List(variable), (map) => {
              val value = map.getOrElse(variable, throw new NotFoundException()).asInstanceOf[PropertyContainer]
              value.getProperty(propName) == expectedValue
            }))

          }
          case NumberLargerThan(variable, propName, comparator) =>
            Some(new FilteringPipe(List(variable), (map) => {
              val entity = map.getOrElse(variable, throw new NotFoundException()).asInstanceOf[PropertyContainer]
              val propValue: Float = entity.getProperty(propName).asInstanceOf[Float]
              propValue > comparator
            }))
        }
      }
    }
  }

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }


  private def createSourcePumps(from: From): Seq[Pipe] = from.fromItems.map(_ match {
    case NodeById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getNodeById))
    case RelatedTo(column, outputNode, outputRel, relType, direction) => new RelatedToPipe(column, outputNode, outputRel, relType, direction)
    case NodeByIndex(varName, idxName, key, value) => {
      val indexHits: java.lang.Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
      val list: List[Node] = indexHits.asScala.toList
      new FromPump(varName, list)
    }
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