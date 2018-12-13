/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.logical.plans.{Apply, Argument, Expand, LoadCSV}
import org.neo4j.cypher.internal.v4_0.util.attribution.SameId
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class LogicalPlanTest extends CypherFunSuite with LogicalPlanningTestSupport  {

  test("single row returns itself as the leafs") {
    val argument = Argument(Set("a"))

    argument.leaves should equal(Seq(argument))
  }

  test("apply with two arguments should return them both") {
    val argument1 = Argument(Set("a"))
    val argument2 = Argument()
    val apply = Apply(argument1, argument2)

    apply.leaves should equal(Seq(argument1, argument2))
  }

  test("apply pyramid should work multiple levels deep") {
    val argument1 = Argument(Set("a"))
    val argument2 = Argument()
    val argument3 = Argument(Set("b"))
    val argument4 = Argument()
    val apply1 = Apply(argument1, argument2)
    val apply2 = Apply(argument3, argument4)
    val metaApply = Apply(apply1, apply2)

    metaApply.leaves should equal(Seq(argument1, argument2, argument3, argument4))
  }

  test("should propagate hasLoadCSV correctly up the plans") {
    val argument = Argument(Set("a"))
    val loadCsv = LoadCSV(argument, null, null, null, None, legacyCsvQuoteEscaping = false, 0)
    loadCsv.propagateHasLoadCSV()
    val apply = Apply(argument, loadCsv)
    apply.propagateHasLoadCSV()

    argument.hasLoadCSV should be(false)
    loadCsv.hasLoadCSV should be(true)
    apply.hasLoadCSV should be(true)
  }

  test("should propagate hasLoadCSV correctly up the plans for longer chain after LOAD CSV") {
    val argument = Argument(Set("a"))
    val loadCsv = LoadCSV(argument, null, null, null, None, legacyCsvQuoteEscaping = false, 0)
    loadCsv.propagateHasLoadCSV()
    val apply = Apply(loadCsv, argument)
    apply.propagateHasLoadCSV()
    val expand = Expand(apply, null, SemanticDirection.INCOMING, null, null, null)
    expand.propagateHasLoadCSV()

    argument.hasLoadCSV should be(false)
    loadCsv.hasLoadCSV should be(true)
    apply.hasLoadCSV should be(true)
    expand.hasLoadCSV should be(true)
  }

  test("should propagate hasLoadCSV correctly up the plans for longer chain before LOAD CSV") {
    val argument1 = Argument(Set("a"))
    val expand = Expand(argument1, null, SemanticDirection.INCOMING, null, null, null)
    expand.propagateHasLoadCSV()
    val argument2 = Argument()
    val apply = Apply(expand, argument2)
    apply.propagateHasLoadCSV()
    val loadCsv = LoadCSV(apply, null, null, null, None, legacyCsvQuoteEscaping = false, 0)
    loadCsv.propagateHasLoadCSV()

    argument1.hasLoadCSV should be(false)
    expand.hasLoadCSV should be(false)
    argument2.hasLoadCSV should be(false)
    apply.hasLoadCSV should be(false)
    loadCsv.hasLoadCSV should be(true)
  }

  test("should propagate hasLoadCSV correctly up the plans without LOAD CSV") {
    val argument1 = Argument(Set("a"))
    val argument2 = Argument()
    val apply = Apply(argument2, argument1)
    apply.propagateHasLoadCSV()
    val expand = Expand(apply, null, SemanticDirection.INCOMING, null, null, null)
    expand.propagateHasLoadCSV()

    argument1.hasLoadCSV should be(false)
    argument2.hasLoadCSV should be(false)
    apply.hasLoadCSV should be(false)
    expand.hasLoadCSV should be(false)
  }

  test("should propagate hasLoadCSV correctly in dup method") {
    val argument = Argument(Set("a"))
    val apply = Apply(argument, argument)

    apply.hasLoadCSV = true
    apply.hasLoadCSV should equal(apply.dup(Seq(argument, argument)).hasLoadCSV)

    apply.hasLoadCSV = false
    apply.hasLoadCSV should equal(apply.dup(Seq(argument, argument)).hasLoadCSV)
  }

  test("should propagate hasLoadCSV correctly in copyPlanWithIdGen method") {
    val argument = Argument(Set("a"))
    val apply = Apply(argument, argument)

    apply.hasLoadCSV = true
    apply.hasLoadCSV should equal(apply.copyPlanWithIdGen(SameId(apply.id)).hasLoadCSV)

    apply.hasLoadCSV = false
    apply.hasLoadCSV should equal(apply.copyPlanWithIdGen(SameId(apply.id)).hasLoadCSV)
  }
}
