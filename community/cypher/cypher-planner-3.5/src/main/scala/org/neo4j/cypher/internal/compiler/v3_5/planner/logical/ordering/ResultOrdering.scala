package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.ordering

import org.neo4j.cypher.internal.ir.v3_5.{AscColumnOrder, DescColumnOrder, RequiredColumnOrder, RequiredOrder}
import org.neo4j.cypher.internal.planner.v3_5.spi.{AscIndexOrder, IndexOrderCapability, NoIndexOrder}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.util.symbols.CypherType

/**
  * This object provides some utility methods around RequiredOrder and ProvidedOrder.
  */
object ResultOrdering {

  /**
    * Checks if a RequiredOrder is satisfied by a ProvidedOrder
    */
  def satisfiedWith(requiredOrder: RequiredOrder, orderedBy: ProvidedOrder): Boolean = {
    requiredOrder.columns.zipAll(orderedBy.columns, null, null).forall {
      case (null, _) => true
      case (_, null) => false
      case ((name, AscColumnOrder), Ascending(id)) => name == id
      case ((name, DescColumnOrder), Descending(id)) => name == id
      case _ => false
    }
  }

  /**
    * @param requiredOrder    the RequiredOrder from the query
    * @param properties       a sequence of the properties of a (composite) index. The sequence is length one for non-composite indexes.
    *                         The tuple contains the property name together with the type that the index query compares against for that
    *                         property. So for `WHERE n.prop = 1 AND n.foo > 'bla'` this will be Seq( ('prop',CTInt), ('foo',CTString) )
    * @param capabilityLookup a lambda function to ask the index for a (sub)-sequence of types for the order capability it provides.
    *                         With the above example, we would ask the index for its ordering capability for Seq(CTInt, CTString).
    *                         In the future we also want to able to ask it for prefix sequences (e.g. just Seq(CTInt)).
    * @return the order that the index guarantees, if possible in accordance with the given required order.
    */
  def withIndexOrderCapability(requiredOrder: RequiredOrder, properties: Seq[(String, CypherType)], capabilityLookup: Seq[CypherType] => IndexOrderCapability): ProvidedOrder = {
    val orderTypes: Seq[CypherType] = properties.map(_._2)
    // TODO: Currently the list of types must match the index properties, but we might (in Cypher) only care or know about a prefix subset, and
    // we are waiting for the kernel to change this behaviour so we can pass in only the known prefix subset of types.
    val orderBehaviorFromIndex: IndexOrderCapability = capabilityLookup(orderTypes)
    orderBehaviorFromIndex match {
      case NoIndexOrder => ProvidedOrder.empty
      case AscIndexOrder =>
        toProvidedOrder(properties.map {case (name, _) => (name, AscColumnOrder)})
      case _ => throw new IllegalStateException("There is no support for this index order: " + orderBehaviorFromIndex)
    }
  }

  private def toProvidedOrder(orderColumns: Seq[(String, RequiredColumnOrder)]): ProvidedOrder =
    ProvidedOrder(orderColumns.map {
      case (name, AscColumnOrder) => Ascending(name)
      case (name, DescColumnOrder) => Descending(name)
    })
}
