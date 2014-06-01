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

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans for all nodes scans") {
    (new given {
      cardinality = {
        case _: AllNodesScan => 1
        case _               => Double.MaxValue
      }
    } planFor "MATCH (n) RETURN n").plan should equal(
      AllNodesScan("n")
    )
  }

  test("should build plans for label scans without compile-time label id") {
    (new given {
      cardinality = {
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
      cardinality = {
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
      NodeIndexSeek("n", "Awesome", "prop", SignedIntegerLiteral("42")_)
    )
  }

  test("should build plans for unique index seek when there is an unique index on the property") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexUniqueSeek("n", "Awesome", "prop", SignedIntegerLiteral("42")_)
    )
  }

  test("should build plans for unique index seek when there is both unique and non-unique indexes to pick") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexUniqueSeek("n", "Awesome", "prop", SignedIntegerLiteral("42")_)
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
}
