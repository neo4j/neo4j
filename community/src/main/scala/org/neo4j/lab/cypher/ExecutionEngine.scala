package org.neo4j.lab.cypher

import commands._
import fsm.{PropertyEquals, FSM}
import pipes.{Pipe, FromPump}
import scala.collection.JavaConverters._
import org.neo4j.graphdb.{NotFoundException, Node, GraphDatabaseService}
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

      val sourcePump = createSourcePumps(from).reduceLeft(_ ++ _)
      val projections = createProjectionTransformers(select)
      val executionPlan = new FSM(sourcePump)

      where match {
        case Some(w) => w.clauses.foreach((c) => {
          c match {
            case StringEquals(variable, propName, value) => {
              val patternNode = executionPlan.bindNode(variable)
              patternNode.addRule(new PropertyEquals(patternNode, propName, value))
              patternNode
            }

          }
        })
        case _ =>
      }

      new Projection(executionPlan, projections)
    }
  }

  def createProjectionTransformers(select: Select): Seq[MapTransformer] = select.selectItems.map((selectItem) => {
    selectItem match {
      case PropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
      case EntityOutput(nodeName) => nodeOutput(nodeName) _
    }
  })

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  private def createSourcePumps(from: From): Seq[Pipe] = from.fromItems.map(_ match {
    case NodeByIndex(varName, idxName, key, value) => {
      val indexHits: java.lang.Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
      val list: List[Node] = indexHits.asScala.toList
      new FromPump(varName, list)
    }
    case NodeById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getNodeById))
  })
}