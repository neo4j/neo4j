package org.neo4j.lab.cypher

import org.neo4j.graphdb.{Node, GraphDatabaseService}

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  def execute(query: Query): List[Node] = query match {
    case Query(select, List(VariableAssignment(variable, from)), where) => {
      getStuffFrom(from).toList
    }
  }

  private def getStuffFrom(fromItems: FromItem*): Seq[Node] = {
    fromItems.head match {
      case NodeById(ids) => ids.map(graph.getNodeById)
    }
  }

}