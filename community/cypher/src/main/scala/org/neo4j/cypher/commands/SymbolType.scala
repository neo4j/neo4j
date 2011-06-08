package org.neo4j.cypher.commands

/**
 * @author mh
 * @since 09.06.11
 */

abstract sealed case class SymbolType(name : String)
case class NodeType(name:String) extends SymbolType(name)
case class RelationshipType(name:String) extends SymbolType(name)
case class PropertyType(name:String) extends SymbolType(name)
case class AggregationType(name:String) extends SymbolType(name)