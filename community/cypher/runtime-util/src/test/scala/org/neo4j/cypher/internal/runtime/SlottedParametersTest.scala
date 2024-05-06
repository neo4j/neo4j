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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Column
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SlottedParametersTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit val idGen: SequentialIdGen = new SequentialIdGen()

  test("should rewrite plan") {
    // given
    val allNodes = AllNodesScan(varFor("x"), Set.empty)
    val predicate = greaterThan(add(parameter("a", symbols.CTAny), parameter("b", symbols.CTAny)), literalInt(42))
    val produceResult = ProduceResult(Selection(Seq(predicate), allNodes), Seq(Column(varFor("x"), Set.empty)))

    // when
    val (newPlan, mapping) = slottedParameters(produceResult)

    // then
    val newPredicate = greaterThan(
      add(ParameterFromSlot(0, "a", symbols.CTAny), ParameterFromSlot(1, "b", symbols.CTAny)),
      literalInt(42)
    )
    mapping should equal(ParameterMapping.empty.updated("a").updated("b"))
    newPlan should equal(ProduceResult.withNoCachedProperties(Selection(Seq(newPredicate), allNodes), Seq(varFor("x"))))
  }

  test("should rewrite plan with multiple occurrences of same parameter") {
    // given
    val allNodes = AllNodesScan(varFor("x"), Set.empty)
    val predicate = greaterThan(add(parameter("a", symbols.CTAny), parameter("a", symbols.CTAny)), literalInt(42))
    val produceResult = ProduceResult.withNoCachedProperties(Selection(Seq(predicate), allNodes), Seq(varFor("x")))

    // when
    val (newPlan, mapping) = slottedParameters(produceResult)

    // then
    val newPredicate = greaterThan(
      add(ParameterFromSlot(0, "a", symbols.CTAny), ParameterFromSlot(0, "a", symbols.CTAny)),
      literalInt(42)
    )
    mapping should equal(ParameterMapping.empty.updated("a"))
    newPlan should equal(ProduceResult.withNoCachedProperties(Selection(Seq(newPredicate), allNodes), Seq(varFor("x"))))
  }

}
