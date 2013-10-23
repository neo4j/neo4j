package org.neo4j.cypher.internal.compiler.v2_0.executionplan

import org.neo4j.cypher.internal.compiler.v2_0.spi.TokenContext

abstract class ExecutionPlan[-T <: TokenContext]
{
  def execute(queryContext: T, params: Map[String, Any])
  def profile(queryContext: T, params: Map[String, Any])

}