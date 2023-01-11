/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.ast.generator.AstGenerator.boolean
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.StdDev
import org.neo4j.cypher.internal.expressions.functions.StdDevP
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.scalacheck.Gen
import org.scalacheck.Gen.frequency
import org.scalacheck.Gen.listOfN
import org.scalacheck.Gen.oneOf

/**
 * Prototype of a generator that generates semantically valid expressions/ASTs.
 */
class SemanticAwareAstGenerator(simpleStrings: Boolean = true, allowedVarNames: Option[Seq[String]] = None)
    extends AstGenerator(simpleStrings, allowedVarNames) {

  private val supportedAggregationFunctions =
    Seq(Avg, Collect, Count, Max, Min, PercentileCont, PercentileDisc, StdDev, StdDevP, Sum)

  // FIXME this generates too many invalid combinations
  def aggregationFunctionInvocation: Gen[FunctionInvocation] = for {
    function <- oneOf(supportedAggregationFunctions)
    signature <- oneOf(function.signatures)
    numArgs = signature.argumentTypes.length
    distinct <- boolean
    args <- listOfN(numArgs, nonAggregatingExpression)
    (ns, name) = function.asFunctionName(pos)
  } yield FunctionInvocation(ns, name, distinct, args.toIndexedSeq)(pos)

  def aggregatingExpression: Gen[Expression] =
    frequency(
      supportedAggregationFunctions.size ->
        aggregationFunctionInvocation,
      1 ->
        _countStar
    )

  def nonAggregatingExpression: Gen[Expression] =
    _expression.suchThat { expr =>
      !expr.containsAggregate && !expr.folder.treeExists {
        case _: FunctionInvocation =>
          true // not interested in randomly-generated functions, we'll just end up with `scala.MatchError: UnresolvedFunction`
        case _: MapProjection =>
          true // org.neo4j.exceptions.InternalException: `MapProjection` should have been rewritten away
      }
    }
}
