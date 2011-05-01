package org.neo4j.lab.cypher.commands

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 19:08 
 */

abstract sealed class SelectItem

case class EntityOutput(name: String) extends SelectItem
case class PropertyOutput(entityName:String, propName:String) extends SelectItem
