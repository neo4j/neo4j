package org.neo4j.cypher.internal.evaluator

import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

trait ExpressionEvaluator {

  def evaluate(expression: String, params: MapValue): AnyValue

}
