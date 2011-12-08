package org.neo4j.cypher

trait ExecutionPlan {
  def execute(params: (String, Any)*): ExecutionResult
  def dumpToString():String
}