package org.neo4j.lab.cypher

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 19:08 
 */

abstract sealed class SelectItem

case class NodeOutput(name: String) extends SelectItem
case class NodePropertyOutput(nodeName:String, propName:String) extends SelectItem
