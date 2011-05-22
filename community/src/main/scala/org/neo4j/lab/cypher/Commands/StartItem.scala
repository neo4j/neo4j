package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.{Node, Relationship, Direction}

abstract sealed class StartItem(val placeholderName:String)

abstract class RelationshipStartItem(varName:String) extends StartItem(varName)

abstract class NodeStartItem(varName:String) extends StartItem(varName)

case class RelationshipById(varName:String, id: Long*) extends RelationshipStartItem(varName)

case class NodeByIndex(varName:String, idxName: String, key:String, value: Any) extends NodeStartItem(varName)

case class RelationshipByIndex(varName:String, idxName: String, value: Any) extends RelationshipStartItem(varName)

case class NodeById(varName:String, id: Long*) extends NodeStartItem(varName)