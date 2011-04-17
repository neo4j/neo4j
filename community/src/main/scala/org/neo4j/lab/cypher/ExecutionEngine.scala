package org.neo4j.lab.cypher

import commands._
import org.neo4j.graphdb.{Node, GraphDatabaseService}
import scala.collection.mutable.HashMap

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  def execute(query: Query): ExecutionResult = query match {
    case Query(select, variableAssignments, where) => {

      val inputNodes = getStuffFrom(variableAssignments)
      val filter: Map[String, (Node) => Boolean] = createFilterFrom(where)

      new ExecutionResult(inputNodes, null, filter)

      //      createSelectOutput(select, inputNodes.filter(filter))
    }
  }

  private def createSelectOutput[T](select: Select, inputNodes: scala.Seq[Node]): List[T] = {
    val selectItem = select.selectItems.head

    val transformer: (Node) => T = selectItem match {
      case NodeOutput(variable) => (n) => n.asInstanceOf[T]
      case NodePropertyOutput(nodeName, propName) => (n) => n.getProperty(propName).asInstanceOf[T]
    }

    inputNodes.map(transformer).toList
  }

  private def getStuffFrom(variableAssignments: List[VariableAssignment]): Map[String, () => Seq[Node]] = {
    val result: HashMap[String, () => scala.Seq[Node]] = new HashMap[String, () => Seq[Node]]()


    variableAssignments.foreach((va) => {
      val f: () => Seq[Node] = va.fromitem match {
        case NodeById(ids) => () => ids.map(graph.getNodeById)
      }

      result += ((va.variable, f))
    })


    result.toMap
  }

  private def createFilterFrom(where: Option[Where]): Map[String, (Node) => Boolean] = where match {
    case None => Map[String, (Node) => Boolean]()
    case Some(w) => {
      val result: HashMap[String, (Node) => Boolean] = new HashMap[String, (Node) => Boolean]()


      w.clauses.foreach((clause) => {

        val tuple = clause match {
          case StringEquals(variable, propName, expectedValue) => (variable, (n: Node) => n.getProperty(propName) == expectedValue)
        }

        result += tuple
      })

      result.toMap
    }
  }
}