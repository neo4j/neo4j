package org.neo4j.lab.cypher

import org.neo4j.graphdb.{Node, GraphDatabaseService}

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {


  def execute[T](query: Query): List[T] = query match {
    case Query(select, List(VariableAssignment(variable, from)), where) => {

      val inputNodes = getStuffFrom(from)
      val filter: (Node) => Boolean = createFilterFrom(where)

      createSelectOutput(select, inputNodes.filter(filter))
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

  private def getStuffFrom(fromItems: FromItem*): Seq[Node] = {
    fromItems.head match {
      case NodeById(ids) => ids.map(graph.getNodeById)
    }
  }

  private def createFilterFrom(where: Option[Where]): (Node) => Boolean = where match {
    case None => (x: Node) => true
    case Some(w) => {
      w.clauses.head match {
        case StringEquals(variable, propName, expectedValue) => (n) => n.getProperty(propName) == expectedValue
      }
    }
  }
}