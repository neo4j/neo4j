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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.AllReducePredicate.AllReduceScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.ReductionStepVariableScope
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AllReducePredicateTest extends CypherFunSuite {

  test("dependencies are calculated correctly") {
    val pos = InputPosition.NONE

    // allReduce(acc = init, iter IN x | acc + iter.prop + external, acc < threshold)
    val stepVariable = Variable("iter")(pos, isIsolated = false)
    val list = Variable("x")(pos, isIsolated = false)
    val init = Variable("init")(pos, isIsolated = false)
    val acc = Variable("acc")(pos, isIsolated = false)
    val external = Variable("external")(pos, isIsolated = false)
    val threshold = Variable("threshold")(pos, isIsolated = false)

    val expr = AllReducePredicate(
      scope = AllReduceScope(
        accumulator = acc,
        reductionStepScope = ReductionStepVariableScope(
          reductionStepVariable = stepVariable.copyId,
          reductionStep = Add(
            Add(acc.copyId, Property(stepVariable.copyId, PropertyKeyName("prop")(pos))(pos))(pos),
            external
          )(pos),
          predicate = LessThan(acc.copyId, threshold)(pos)
        )(pos)
      )(pos),
      init = init,
      list = list
    )(pos)

    expr.dependencies.map(Ref.apply) shouldEqual Set(
      init,
      list,
      threshold,
      external
    ).map(Ref.apply)
  }
}
