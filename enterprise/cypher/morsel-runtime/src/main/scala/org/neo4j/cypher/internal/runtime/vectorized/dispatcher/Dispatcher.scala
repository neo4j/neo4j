package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

trait Dispatcher {
  def execute[E <: Exception](operators: Pipeline,
                              queryContext: QueryContext,
                              params: MapValue)(visitor: QueryResultVisitor[E]): Unit
}

