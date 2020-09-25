package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

/**
  * Special aggregating function for performing collect() but preserving nulls. Not exposed in Cypher.
 *
 * @param expr the expression to collect
  */
case class CollectAll(expr: Expression)(val position: InputPosition) extends Expression
