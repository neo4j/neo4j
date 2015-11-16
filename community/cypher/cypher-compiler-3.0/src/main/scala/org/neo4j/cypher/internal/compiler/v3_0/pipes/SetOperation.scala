package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CastSupport, IsMap}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.makeValueNeoSafe
import org.neo4j.cypher.internal.compiler.v3_0.spi.{QueryContext, Operations}
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}

import scala.collection.Map

sealed trait SetOperation {
  def set(executionContext: ExecutionContext, state: QueryState): Unit

  def name: String
}

object SetOperation {

  private[pipes] def setProperty[T <: PropertyContainer](context: ExecutionContext, state: QueryState, ops: Operations[T],itemId: Long, propertyKey: LazyPropertyKey, expression: Expression) = {
    val queryContext = state.query
    val maybePropertyKey = propertyKey.id(queryContext).map(_.id) // if the key was already looked up
    val propertyId = maybePropertyKey
        .getOrElse(queryContext.getOrCreatePropertyKeyId(propertyKey.name)) // otherwise create it
    val value = makeValueNeoSafe(expression(context)(state))

    if (value == null) ops.removeProperty(itemId, propertyId)
    else ops.setProperty(itemId, propertyId, value)
  }

  private[pipes] def setPropertiesFromMap[T <: PropertyContainer](qtx: QueryContext, ops: Operations[T], itemId: Long,
                              map: Map[Int, Any], removeOtherProps: Boolean) {
    /*Set all map values on the property container*/
    for ((k, v) <- map) {
      if (v == null)
        ops.removeProperty(itemId, k)
      else
        ops.setProperty(itemId, k, makeValueNeoSafe(v))
    }

    val properties = ops.propertyKeyIds(itemId).filterNot(map.contains).toSet

    /*Remove all other properties from the property container ( SET n = {prop1: ...})*/
    if (removeOtherProps) {
      for (propertyKeyId <- properties) {
        ops.removeProperty(itemId, propertyKeyId)
      }
    }
  }

  private[pipes] def toMap(executionContext: ExecutionContext, state: QueryState, expression: Expression) = {
    /* Make the map expression look like a map */
    val qtx = state.query
    expression(executionContext)(state) match {
      case IsMap(createMapFrom) => SetOperation.propertyKeyMap(qtx, createMapFrom(qtx))
      case x => throw new CypherTypeException(s"Expected $expression to be a map, but it was :`$x`")
    }
  }

  private def propertyKeyMap(qtx: QueryContext, map: Map[String, Any]): Map[Int, Any] = {
    var builder = Map.newBuilder[Int, Any]

    for ((k, v) <- map) {
      if (v == null) {
        val optPropertyKeyId = qtx.getOptPropertyKeyId(k)
        if (optPropertyKeyId.isDefined) {
          builder += optPropertyKeyId.get -> v
        }
      }
      else {
        builder += qtx.getOrCreatePropertyKeyId(k) -> v
      }
    }

    builder.result()
  }
}

case class SetNodePropertyOperation(nodeName: String, propertyKey: LazyPropertyKey, expression: Expression) extends SetOperation{

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value = executionContext.get(nodeName).get
    if (value != null) {
      val node = CastSupport.castOrFail[Node](value)
      SetOperation.setProperty(executionContext, state, state.query.nodeOps, node.getId, propertyKey,
        expression)
    }
  }

  override def name = "SetNodeProperty"
}

case class SetRelationshipPropertyOperation(relName: String, propertyKey: LazyPropertyKey, expression: Expression) extends SetOperation{

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value = executionContext.get(relName).get
    if (value != null) {
      val relationship = CastSupport.castOrFail[Relationship](value)
      SetOperation.setProperty(executionContext, state, state.query.relationshipOps,
        relationship.getId, propertyKey, expression)
    }
  }

  override def name = "SetRelationshipProperty"

}

case class SetNodePropertyFromMapOperation(nodeName: String, expression: Expression, removeOtherProps: Boolean) extends SetOperation{

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value = executionContext.get(nodeName).get
    if (value != null) {
      val map = SetOperation.toMap(executionContext, state, expression)

      val node = CastSupport.castOrFail[Node](value)
      SetOperation.setPropertiesFromMap(state.query, state.query.nodeOps, node.getId, map, removeOtherProps)
    }
  }

  override def name = "SetNodePropertyFromMap"
}

case class SetRelationshipPropertyFromMapOperation(relName: String, expression: Expression, removeOtherProps: Boolean) extends SetOperation{

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value = executionContext.get(relName).get
    if (value != null) {
      val map = SetOperation.toMap(executionContext, state, expression)

      val relationship = CastSupport.castOrFail[Relationship](value)
      SetOperation
        .setPropertiesFromMap(state.query, state.query.relationshipOps, relationship.getId, map, removeOtherProps)
    }
  }

  override def name = "SetRelationshipPropertyFromMap"

}

case class SetLabelsOperation(nodeName: String, labels: Seq[LazyLabel]) extends SetOperation {

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value = executionContext.get(nodeName).get
    if (value != null) {
      val nodeId = CastSupport.castOrFail[Node](value).getId
      val labelIds = labels.map(l => {
        val maybeLabelId = l.id(state.query).map(_.id)
        maybeLabelId getOrElse state.query.getOrCreateLabelId(l.name)
      })
      state.query.setLabelsOnNode(nodeId, labelIds.iterator)
    }
  }

  override def name = "SetLabels"
}


