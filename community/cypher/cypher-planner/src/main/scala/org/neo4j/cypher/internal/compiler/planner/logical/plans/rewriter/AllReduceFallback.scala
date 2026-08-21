/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.AllReducePredicate
import org.neo4j.cypher.internal.expressions.AllReduceSingletonPredicate
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

case class AllReduceFallback(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator
) extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  /**
   * Translate the `allReduce` function to a post-filter `reduce` functions that are supported in runtime
   *
   * allReduce(acc = 0, step IN n | acc + step.x, acc < 12)
   *
   * gets translated into
   *
   * reduce(
   *  acc = {accumulator: 0, result: true},
   *  step IN n |
   *    CASE
   *      WHEN acc.result = false THEN acc
   *      ELSE [anon_0 IN [acc.accumulator + step.x] | {accumulator: anon_0, result: acc.result AND anon_0 < 12}][0]
   *    END
   * ).result
   *
   * The accumulator variable of the allReduce() is reused for the reduce().
   * To avoid confusion, lets call it state in the reduce().
   * The state consists of the `accumulator` value and the current `result`.
   * The accumulator in the state gets updated for each value in the list as long as the result in the state is true.
   * Once the result in the state is false, the step function and predicate function of the allReduce() do not need to
   * be executed anymore.
   * Once all elements in the list have been processed the result from the state will be returned.
   *
   * The ListComprehension in the ELSE clause of the CASE expression is used to avoid computing the step function of the allReduce
   * twice (once to obtain the new accumulator value and one to obtain the new result value).
   *
   * @param allReducePredicate the allReduce function to be translated
   * @return an expression equivalent to the allReduce function but in terms of expressions that are supported in runtime
   */
  def rewriteAllReduce(
    allReducePredicate: AllReducePredicate
  ): Expression = {
    val pos = allReducePredicate.position
    val nextAcc = Variable(anonymousVariableNameGenerator.nextName)(pos, isIsolated = false)

    // Reuse the accumulator variable from the allReduce()
    // In the reduce we will use it to store two elements and call it `state` instead of `accumulator`. It will contain:
    // - the accumulator value
    // - boolean value holding the intermediate result (once the result is `false` we do not need to execute the step and predicate functions anymore)
    val state = allReducePredicate.accumulator
    val stateAcc = PropertyKeyName("accumulator")(pos)
    val stateResult = PropertyKeyName("result")(pos)

    val caseExpr = CaseExpression(
      expression = None,
      alternatives = List(
        // if (state.result == false) state
        Equals(Property(state, stateResult)(pos), False()(pos.zeroLength))(pos) -> state
      ),
      default = Some(
        // [nextAcc IN [reductionStepExpression(state.accumulator, reductionStepScopeSingletonVariable)] |
        //  { accumulator: nextAcc, continue: state.continue AND predicate(nextAcc) }
        // ][0]
        ContainerIndex(
          ListComprehension(
            variable = nextAcc,
            expression = ListLiteral(
              Seq(
                // reductionStepExpression(state.accumulator, reductionStepScopeSingletonVariable)
                allReducePredicate.reductionStep
                  .replaceAllOccurrencesBy(
                    allReducePredicate.accumulator,
                    Property(state, stateAcc)(pos)
                  )
              )
            )(pos),
            innerPredicate = None,
            extractExpression = Some(
              // { accumulator: nextAcc, result: state.continue AND predicate(nextAcc) }
              MapExpression(
                Seq(
                  (stateAcc, nextAcc),
                  (
                    stateResult,
                    Ands(ListSet(
                      // state.result
                      Property(state, stateResult)(pos),
                      // predicate(nextAcc)
                      allReducePredicate.predicate.replaceAllOccurrencesBy(
                        allReducePredicate.accumulator,
                        nextAcc
                      )
                    ))(pos)
                  )
                )
              )(pos)
            )
          )(pos),
          SignedDecimalIntegerLiteral("0")(pos.zeroLength)
        )(pos)
      )
    )(pos)

    val reduceExpr = ReduceExpression(
      ReduceScope(
        accumulator = state,
        variable = allReducePredicate.scope.reductionStepScope.reductionStepVariable,
        expression = caseExpr
      )(pos),
      init = MapExpression(Seq(
        // Provide the initial value for the accumulator and set the intermediate result to true
        stateAcc -> allReducePredicate.init,
        stateResult -> True()(pos.zeroLength)
      ))(pos),
      list = allReducePredicate.list
    )(pos)

    // Only retrieve the result from the state (accumulator) or the reduce()
    Property(reduceExpr, stateResult)(pos)
  }

  override val innerRewriter: Rewriter = Rewriter.lift {
    case allReduce: AllReducePredicate => rewriteAllReduce(allReduce)
    case allReduceSingleton: AllReduceSingletonPredicate =>
      // Fallback for leftover AllReduceSingletonPredicate that wasn't rewritten by AllReduceSingletonRewriter.
      // This can happen when the predicate ends up in a nested expression (e.g. inside an EXISTS subquery
      // used as a CASE property value) where the LogicalPlan-only traversal of AllReduceSingletonRewriter
      // cannot reach it.
      // Semantics: single-step reduction - compute the next accumulator value from reductionStep
      // and check the predicate on it.
      allReduceSingleton.predicate.replaceAllOccurrencesBy(
        allReduceSingleton.accumulator,
        allReduceSingleton.reductionStep
      )
  }

  private val instance: Rewriter = bottomUp(innerRewriter)
}
