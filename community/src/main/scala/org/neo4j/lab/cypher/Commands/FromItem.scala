package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

abstract sealed class FromItem

case class NodeById(id: List[Long]) extends FromItem

case class RelationshipById(id: Long*) extends FromItem

case class NodeByIndex(idxName: String, key:String, value: Any) extends FromItem

case class RelationshipByIndex(idxName: String, value: Any) extends FromItem

case class RelatedTo(column:String, relationType:String, direction:Direction) extends FromItem