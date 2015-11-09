package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, MapExpression, PropertyKeyName}

/*
 * Used to simplify finding overlap between writing and reading properties
 */
sealed trait CreatesPropertyKeys {
  def overlaps(propertyKeyName: PropertyKeyName): Boolean
}

/*
 * CREATE (a:L)
 */
case object CreatesNoPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = false
}

/*
 * CREATE ({prop1: 42, prop2: 42})
 */
case class CreatesKnownPropertyKeys(keys: Set[PropertyKeyName]) extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName): Boolean = keys(propertyKeyName)
}

/*
 * CREATE ({props})
 */
case object CreatesUnknownPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = true
}

object CreatesPropertyKeys {
  def apply(properties: Seq[Expression]) = {
    //CREATE ()
    if (properties.isEmpty) CreatesNoPropertyKeys
    else {
      val knownProp: Seq[Seq[(PropertyKeyName, Expression)]] = properties.collect {
        case MapExpression(props) => props
      }
      //all prop keys are known, CREATE ({prop1:1, prop2:2})
      if (knownProp.size == properties.size) CreatesKnownPropertyKeys(knownProp.flatMap(_.map(s => s._1)).toSet)
      //props created are not known, e.g. CREATE ({props})
      else CreatesUnknownPropertyKeys
    }
  }
}

