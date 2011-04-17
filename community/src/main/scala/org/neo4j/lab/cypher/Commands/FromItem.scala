package org.neo4j.lab.cypher.commands

abstract sealed class FromItem

case class NodeById(id: List[Long]) extends FromItem

case class RelationshipById(id: Long*) extends FromItem

case class NodeByIndex(idxName: String, value: Any) extends FromItem

case class RelationshipByIndex(idxName: String, value: Any) extends FromItem