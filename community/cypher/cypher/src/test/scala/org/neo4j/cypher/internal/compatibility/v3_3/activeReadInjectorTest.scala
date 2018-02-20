/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{PropertyKeyName, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.scalatest.Matchers

class activeReadInjectorTest extends CypherFunSuite with Matchers with LogicalPlanningTestSupport2 {

  test("does nothing for non-MERGE plan") {

    val input = EmptyResult(AllNodesScan("a", Set.empty))

    activeReadInjector.apply(input) should equal(input)
  }

  test("should inject into single merge node") {
    val input =
      EmptyResult(
        AntiConditionalApply(
          Optional(
            AllNodesScan("a", Set.empty)
          ),
          MergeCreateNode(Argument(), "a", Seq.empty, None),
          Seq("a")
        )
      )

    activeReadInjector.apply(input) should equal(
      EmptyResult(
        AntiConditionalApply(
          Optional(
            ActiveRead(
              AllNodesScan("a", Set.empty)
            )
          ),
          MergeCreateNode(Argument(), "a", Seq.empty, None),
          Seq("a")
        )
      )
    )
  }

  test("should inject into single merge node with on match") {
    // GIVEN
    val onMatch =
      ConditionalApply(
        Optional(
          AllNodesScan("a", Set.empty)
        ),
        SetLabels(
          Argument(Set("a")),
          "a",
          Seq(lblName("L"))
        ),
        Seq("a")
      )

    val createAndOnCreate =
      SetNodeProperty(
        MergeCreateNode(
          Argument(), "a", Seq.empty, None
        ),
        "a",
        PropertyKeyName("prop")(pos),
        SignedDecimalIntegerLiteral("1")(pos)
      )

    val input =
      EmptyResult(
        AntiConditionalApply(onMatch, createAndOnCreate, Seq("a"))
      )

    // EXPECT
    val expectedOnMatch =
      ConditionalApply(
        Optional(
          ActiveRead(
            AllNodesScan("a", Set.empty)
          )
        ),
        SetLabels(
          Argument(Set("a")),
          "a",
          Seq(lblName("L"))
        ),
        Seq("a")
      )

    // THEN
    activeReadInjector.apply(input) should equal(
      EmptyResult(
        AntiConditionalApply(expectedOnMatch, createAndOnCreate, Seq("a"))
      )
    )
  }

  private def activeReadInjector = {
    ActiveReadInjector(Attributes(idGen))
  }
}
