/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_1.commands.{ManyQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_1.planner.BeLikeMatcher._

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans for all nodes scans") {
    (new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 1
        case _               => Double.MaxValue
      }
    } planFor "MATCH (n) RETURN n").plan should equal(
      AllNodesScan("n")
    )
  }

  test("should build plans for label scans without compile-time label id") {
    (new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 1000
        case _: NodeByIdSeek => 2
        case _: NodeByLabelScan => 1
        case _ => Double.MaxValue
      }
    } planFor "MATCH (n:Awesome) RETURN n").plan should equal(
      NodeByLabelScan("n", Left("Awesome"))
    )
  }

  test("should build plans for label scans with compile-time label id") {
    implicit val plan = new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 1000
        case _: NodeByIdSeek => 2
        case _: NodeByLabelScan => 1
        case _ => Double.MaxValue
      }
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) RETURN n"

    plan.plan should equal(
      NodeByLabelScan("n", Right(labelId("Awesome")))
    )
  }

  test("should build plans for index seek when there is an index on the property") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexSeek(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        SingleQueryExpression(SignedIntegerLiteral("42")(pos))
      )
    )
  }

  test("should build plans for unique index seek when there is an unique index on the property") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexUniqueSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedIntegerLiteral("42")_))
    )
  }

  test("should build plans for unique index seek when there is both unique and non-unique indexes to pick") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexUniqueSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(1)), SingleQueryExpression(SignedIntegerLiteral("42")_))
    )
  }

  test("should build plans for node by ID mixed with label scan when node by ID is cheaper") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n").plan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", Seq(SignedIntegerLiteral("42")_))
      )
    )
  }

  test("should build plans for node by ID when the predicate is IN") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n").plan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", Seq(SignedIntegerLiteral("42")_, SignedIntegerLiteral("64")_))
      )
    )
  }

  test("should build plans for index seek when there is an index on the property and an IN predicate") {
    (new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n").plan should beLike {
      case NodeIndexSeek(
              IdName("n"),
              LabelToken("Awesome", _),
              PropertyKeyToken("prop", _),
              ManyQueryExpression(Collection(_))) => ()
    }
  }

  test("should not use indexes for large collections") {
    // Index selectivity is 0.08, and label selectivity is 0.2.
    // So if we have 3 elements in the collection, we should not use the index.

    (new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5,6,7,8,9,10] RETURN n").plan should beLike {
      case _: Selection => ()
    }
  }

  test("should use indexes for large collections if it is a unique index") {
    // Index selectivity is 0.08, and label selectivity is 0.2.
    // So if we have 3 elements in the collection, we should not use the index.

    (new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5,6,7,8,9,10] RETURN n").plan should beLike {
      case _: NodeIndexUniqueSeek => ()
    }
  }
}
