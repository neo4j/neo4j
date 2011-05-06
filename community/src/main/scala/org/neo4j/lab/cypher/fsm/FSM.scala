package org.neo4j.lab.cypher.fsm

import org.neo4j.lab.cypher.pipes.Pipe
import java.lang.String
import org.neo4j.graphdb.PropertyContainer

/**
 * Created by Andres Taylor
 * Date: 5/3/11
 * Time: 22:03 
 */

class FSM(source: Pipe) extends Pipe {
  var nodes = new scala.collection.mutable.LinkedList[PatternNode]

  def bindNode(name: String): PatternNode = {
    val pNode = new PatternNode(name)
    nodes = nodes ++ List(pNode)
    pNode
  }

  def foreach[U](f: (Map[String, Any]) => U) {
    val filteredMatches = source.filter((m) => {
      nodes.isEmpty || nodes.head.matches(m)
    })
    filteredMatches.foreach(f.apply(_))
  }

  def columnNames: List[String] = source.columnNames

  def dependsOn: List[String] = List()
}

abstract class Rule {
  def isMatch(state: Map[String, Any]): Boolean
}

class PatternNode(name: String) {
  var rules: List[Rule] = List[Rule]()

  def addRule(rule: Rule) {
    rules = rules ++ List(rule)
  }

  def getPropertyValue(key: String, currentState: Map[String, Any]): Any = currentState.get(name).get.asInstanceOf[PropertyContainer].getProperty(key)

  def matches(state: Map[String, Any]): Boolean = rules.forall(_.isMatch(state))
}

class PropertyEquals[T](node: PatternNode, propName: String, value: T) extends Rule {
  def isMatch(currentState: Map[String, Any]): Boolean = node.getPropertyValue(propName, currentState) == value
}

