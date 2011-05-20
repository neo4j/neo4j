package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

abstract sealed class StartItem(val placeholderName:String)


case class RelationshipById(varName:String, id: Long*) extends StartItem(varName)

case class NodeByIndex(varName:String, idxName: String, key:String, value: Any) extends StartItem(varName)

case class RelationshipByIndex(varName:String, idxName: String, value: Any) extends StartItem(varName)

case class NodeById(varName:String, id: Long*) extends StartItem(varName)