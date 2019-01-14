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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_4.{SimplePatternLength, VarPatternLength}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  implicit val windowsSafe = WindowsStringSafe

  test("should build plans for simple WITH that adds a constant to the rows") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")._2
    val expected =
      Projection(
        Limit(
          AllNodesScan("a", Set.empty),
          SignedDecimalIntegerLiteral("1")(pos),
          DoNotIncludeTies
        ),
        Map[String, Expression]("b" -> SignedDecimalIntegerLiteral("1") _)
      )

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`")._2
    val expected = Limit(
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
        ),
        "a", OUTGOING, List(), "b", "r1", ExpandAll
      ),
      SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
    )

    result should equal(expected)
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1")._2
    val expected = Selection(
      Seq(In(Property(Variable("r1")(pos), PropertyKeyName("prop")(pos))(pos), ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos)),
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
        ),
        "a", OUTGOING, List(), "b", "r1", ExpandAll
      )
    )

    result should equal(expected)
  }

  test("should build plans for two matches separated by WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b")._2
    val expected = Expand(
      Limit(
        AllNodesScan("a", Set()),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      "a", OUTGOING, List(), "b", "r", ExpandAll
    )

    result should equal(expected)
  }

  test("should build plans that project endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("r")),
        "r", "u", startInScope = false, "v", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "b", "r")),
        "r", "a", startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = false, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched directed var length relationship arguments") {
    val result = planFor("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        VarExpand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, OUTGOING, List(), "b", "r", VarPatternLength(1, None), ExpandAll, "r_NODES", "r_RELS", True()(pos), True()(pos), Seq()
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, VarPatternLength(1, None)
      )
    )

    result should equal(expected)
  }
}
