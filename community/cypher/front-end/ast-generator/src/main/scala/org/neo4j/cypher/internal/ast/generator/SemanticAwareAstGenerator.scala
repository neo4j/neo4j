/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    functionName = function.asFunctionName(pos)
  } yield FunctionInvocation(functionName, distinct, args.toIndexedSeq)(pos)

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
