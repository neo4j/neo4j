package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

abstract sealed class FromItem(varNames:String*)

case class NodeById(varName:String, id: Long*) extends FromItem(varName)

case class RelationshipById(varName:String, id: Long*) extends FromItem(varName)

case class NodeByIndex(varName:String, idxName: String, key:String, value: Any) extends FromItem(varName)

case class RelationshipByIndex(varName:String, idxName: String, value: Any) extends FromItem(varName)

case class RelatedTo(inputColumn:String, outputNode:String, outputRel:String, relationType:String, direction:Direction) extends FromItem(outputNode, outputRel)