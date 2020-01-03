/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, ProduceResult, Selection}
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SlottedParametersTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit val idGen: SequentialIdGen = new SequentialIdGen()

  test("should rewrite plan") {
    //given
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = greaterThan(add(parameter("a", symbols.CTAny), parameter("b", symbols.CTAny)), literalInt(42))
    val produceResult = ProduceResult(Selection(Seq(predicate), allNodes), Seq("x"))


    //when
    val (newPlan, mapping) = slottedParameters(produceResult)

    //then
    val newPredicate = greaterThan(add(ParameterFromSlot(0, "a", symbols.CTAny), ParameterFromSlot( 1, "b", symbols.CTAny)), literalInt(42))
    mapping should equal(ParameterMapping.empty.updated("a").updated("b"))
    newPlan should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
  }

  test("should rewrite plan with multiple occurrences of same parameter") {
    //given
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = greaterThan(add(parameter("a", symbols.CTAny), parameter("a", symbols.CTAny)), literalInt(42))
    val produceResult = ProduceResult(Selection(Seq(predicate), allNodes), Seq("x"))


    //when
    val (newPlan, mapping) = slottedParameters(produceResult)

    //then
    val newPredicate = greaterThan(add(ParameterFromSlot(0, "a", symbols.CTAny), ParameterFromSlot( 0, "a", symbols.CTAny)), literalInt(42))
    mapping should equal(ParameterMapping.empty.updated("a"))
    newPlan should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
  }

}
