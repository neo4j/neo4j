/**
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
package org.neo4j.cypher.internal.compiler.v2_1.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.{scalaDocBuilder, toStringDocBuilder, DocBuilderTestSuite}

class PlannerQueryDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder = plannerQueryDocBuilder orElse plannerDocBuilder orElse scalaDocBuilder orElse toStringDocBuilder

  test("renders tail free empty planner query") {
    format(PlannerQuery(
      graph = QueryGraph(),
      horizon = QueryProjection.empty
    )) should equal("GIVEN * RETURN *")
  }

  test("renders tail free non-empty planner query") {
    format(PlannerQuery(
      graph = QueryGraph(patternNodes = Set(IdName("a"))),
      horizon = RegularQueryProjection(projections = Map("a" -> SignedDecimalIntegerLiteral("1") _))
    )) should equal("GIVEN * MATCH (a) RETURN SignedDecimalIntegerLiteral(\"1\") AS `a`")
  }

  test("render planner query with tail") {
    format(PlannerQuery(
      graph = QueryGraph(patternNodes = Set(IdName("a"))),
      horizon = RegularQueryProjection(projections = Map("a" -> SignedDecimalIntegerLiteral("1") _)),
      tail = Some(PlannerQuery.empty)
    )) should equal("GIVEN * MATCH (a) WITH SignedDecimalIntegerLiteral(\"1\") AS `a` GIVEN * RETURN *")
  }
}
