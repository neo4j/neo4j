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
      val selectItem = select.selectItems.head

      val transformer: (Node) => T = selectItem match {
        case NodeOutput(variable) => (n) => n.asInstanceOf[T]
        case NodePropertyOutput(nodeName, propName) => (n) => n.getProperty(propName).asInstanceOf[T]
      }

      inputNodes.map(transformer).toList
    }
  }

  private def getStuffFrom(fromItems: FromItem*): Seq[Node] = {
    fromItems.head match {
      case NodeById(ids) => ids.map(graph.getNodeById)
    }
  }


  //
  //  class ResultPump[T]()
  //
  //  class ExecutionResult(columnNames: List[String], pumps: List[ResultPump]) {
  //    def getResult(): List[Any] = {
  //
  //    }
  //  }

}