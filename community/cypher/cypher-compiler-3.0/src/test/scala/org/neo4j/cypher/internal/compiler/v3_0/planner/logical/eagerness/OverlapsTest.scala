/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.scalatest.matchers.{MatchResult, Matcher}

class OverlapsTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport with PlannerQueryTestSupport {


  test("two MATCHes with property followed by SET property should overlap") {
    // given
    val queryGraph = (matchNode("a") withPredicate propEquality("a", "prop", 42)) ++ matchNode("b") withMutation setNodeProperty("b", "prop")

    // when
    queryGraph.updates should overlap(queryGraph.reads)
  }

  class OverlapsMatcher(read: Read) extends Matcher[Update] {

    def apply(update: Update) = {

      MatchResult(
        update overlaps read,
        s"""Update did not overlap read""",
        s"""Update did overlap read"""
      )
    }
  }

  private def overlap(read: Read) = new OverlapsMatcher(read)
}
