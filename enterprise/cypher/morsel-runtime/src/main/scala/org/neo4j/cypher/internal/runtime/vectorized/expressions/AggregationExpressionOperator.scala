package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.util.v3_4.SyntaxException
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.values.AnyValue

abstract class AggregationExpressionOperator  extends Expression {

  def apply(ctx: ExecutionContext, state: OldQueryState) =
    throw new UnsupportedOperationException("Aggregations should not be used like this.")

  def createAggregationMapper: AggregationMapper
  def createAggregationReducer: AggregationReducer
}

abstract class AggregationExpressionOperatorWithInnerExpression(inner:Expression) extends AggregationExpressionOperator {
  if(inner.containsAggregate)
    throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")

  if(! inner.isDeterministic)
    throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

  def expectedInnerType: CypherType

  def arguments = Seq(inner)

  def symbolTableDependencies: Set[String] = inner.symbolTableDependencies
}

trait AggregationMapper {
  def map(data: MorselExecutionContext, state: OldQueryState): Unit
  def result: AnyValue
}

trait AggregationReducer {
  def reduce(value: AnyValue): Unit
  def result: AnyValue
}
