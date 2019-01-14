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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_4.expressions.{LabelName, PropertyKeyName, RelTypeName, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.scalatest.Matchers

class ActiveReadInjectorTest extends CypherFunSuite with Matchers with LogicalPlanningTestSupport2 {

  test("does nothing for non-MERGE plan") {

    val input = EmptyResult(AllNodesScan("a", Set.empty))

    activeReadInjector(input) should equal(input)
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

    activeReadInjector(input) should equal(
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
    activeReadInjector(input) should equal(
      EmptyResult(
        AntiConditionalApply(expectedOnMatch, createAndOnCreate, Seq("a"))
      )
    )
  }

  test("should inject into unbound relationship merge") {
    // GIVEN
    val expand =
      Expand(
        NodeByLabelScan(
          "a", LabelName("A")(pos), Set.empty
        ),
        "a", OUTGOING, Seq(RelTypeName("R")(pos)), "b", "r"
      )

    val onCreate =
      MergeCreateRelationship(
        MergeCreateNode(
          MergeCreateNode(
            Argument(),
            "a", Seq(LabelName("A")(pos)), None
          ),
          "b", Seq.empty, None
        ),
        "r", "a", RelTypeName("R")(pos), "b", None
      )

    // WHEN
    val input =
      EmptyResult(
        AntiConditionalApply(
          Optional(expand),
          onCreate, Seq("a", "b", "r")
        )
      )

    // THEN
    activeReadInjector(input) should equal(
      EmptyResult(
        AntiConditionalApply(
          Optional(
            ActiveRead(expand)
          ),
          onCreate, Seq("a", "b", "r")
        )
      )
    )
  }

  test("should inject into bound relationship merge") {
    // GIVEN
    val scans =
      CartesianProduct(
        AllNodesScan("n", Set()),
        AllNodesScan("m", Set())
      )

    val expand =
      Expand(
        Argument(Set("n", "m")),
        "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto
      )

    val lockedExpand =
      Expand(
        LockNodes(
          Argument(Set("n", "m")),
          Set("n", "m")
        ),
        "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto
      )

    val onCreate =
      MergeCreateRelationship(
        Argument(Set("n", "m")),
        "r", "n", RelTypeName("T")(pos), "m", None)

    // WHEN
    val input =
      EmptyResult(
        Apply(
          scans,
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(expand, Set("n", "m")),
              Optional(lockedExpand, Set("n", "m")),
              Vector("r")
            ),
            onCreate,
            Vector("r")
          )
        )
      )

    // THEN
    activeReadInjector(input) should equal(
      EmptyResult(
        Apply(
          scans,
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(ActiveRead(expand), Set("n", "m")),
              Optional(ActiveRead(lockedExpand), Set("n", "m")),
              Vector("r")
            ),
            onCreate,
            Vector("r")
          )
        )
      )
    )
  }

  test("should inject into bound relationship merge with ON MATCH") {
    // GIVEN
    val scans =
      CartesianProduct(
        AllNodesScan("n", Set()),
        AllNodesScan("m", Set())
      )

    val expand =
      Expand(
        Argument(Set("n", "m")),
        "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto
      )

    val lockedExpand =
      Expand(
        LockNodes(
          Argument(Set("n", "m")),
          Set("n", "m")
        ),
        "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto
      )

    val onCreate =
      MergeCreateRelationship(
        Argument(Set("n", "m")),
        "r", "n", RelTypeName("T")(pos), "m", None)

    val onMatch =
      SetRelationshipPropery(
        Argument(Set("n", "m", "r")),
        "r", PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("3")(pos)
      )

    // WHEN
    val input =
      EmptyResult(
        Apply(
          scans,
          AntiConditionalApply(
            ConditionalApply(
              AntiConditionalApply(
                Optional(expand, Set("n", "m")),
                Optional(lockedExpand, Set("n", "m")),
                Vector("r")
              ),
              onMatch,
              Vector("r")
            ),
            onCreate,
            Vector("r")
          )
        )
      )

    // THEN
    activeReadInjector(input) should equal(
      EmptyResult(
        Apply(
          scans,
          AntiConditionalApply(
            ConditionalApply(
              AntiConditionalApply(
                Optional(ActiveRead(expand), Set("n", "m")),
                Optional(ActiveRead(lockedExpand), Set("n", "m")),
                Vector("r")
              ),
              onMatch,
              Vector("r")
            ),
            onCreate,
            Vector("r")
          )
        )
      )
    )
  }

  private def activeReadInjector = {
    ActiveReadInjector(Attributes(idGen))
  }
}
