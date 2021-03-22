package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateNodeCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateRelationshipCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeCreateNodePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeCreateRelationshipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

trait InterpretedSideEffect {
  def execute(row: CypherRow, state: QueryState)
}

case class CreateNode(command: CreateNodeCommand) extends InterpretedSideEffect {
  override def execute(row: CypherRow,
                       state: QueryState): Unit = {
    val query = state.query
    val labelIds = command.labels.map(_.getOrCreateId(query)).toArray
    val node = query.createNode(labelIds)
    command.properties.foreach(p => p.apply(row, state) match {
      case IsMap(map) =>
        map(state).foreach((k: String, v: AnyValue) => {
          if (v eq Values.NO_VALUE) MergeCreateNodePipe.handleNoValue(command.labels.map(_.name), k)
          else {
            val propId = query.getOrCreatePropertyKeyId(k)
            query.nodeOps.setProperty(node.id(), propId, makeValueNeoSafe(v))
          }
        })

      case value =>
        throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

    })
    row.set(command.idName, node)
  }
}

case class CreateRelationship(command: CreateRelationshipCommand) extends InterpretedSideEffect {
  override def execute(row: CypherRow,
                       state: QueryState): Unit = {
    val typeId = state.query.getOrCreateRelTypeId(command.relType.name)
    val start = getNode(row, command.idName, command.startNode, state.lenientCreateRelationship)
    val end = getNode(row, command.idName, command.endNode, state.lenientCreateRelationship)

    val relationship = state.query.createRelationship(start.id(), end.id(), typeId)
    command.properties.foreach(p => p.apply(row, state) match {
      case IsMap(map) =>
        map(state).foreach((k: String, v: AnyValue) => {
          if (v eq Values.NO_VALUE) MergeCreateRelationshipPipe.handleNoValue(command.startNode, command.relType.name, command.endNode, k)
          else {
            val propId = state.query.getOrCreatePropertyKeyId(k)
            state.query.relationshipOps.setProperty(relationship.id(), propId, makeValueNeoSafe(v))
          }
        })

      case value =>
        throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

    })
    row.set(command.idName, relationship)
  }


  private def getNode(row: CypherRow, relName: String, name: String, lenient: Boolean): NodeValue =
    row.getByName(name) match {
      case n: NodeValue => n
      case IsNoValue() =>
        if (lenient) null
        else throw new InternalException(LenientCreateRelationship.errorMsg(relName, name))
      case x => throw new InternalException(s"Expected to find a node at '$name' but found instead: $x")
    }
}


