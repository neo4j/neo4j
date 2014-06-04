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
package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.ast.{DescSortItem, AscSortItem, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryProjection
import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.{plannerDocBuilder, queryProjectionDocBuilder}

class QueryProjectionDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder =
    queryProjectionDocBuilder("WITH") orElse
    plannerDocBuilder orElse
    scalaDocBuilder orElse
    toStringDocBuilder

  test("renders star projections") {
    format(QueryProjection.empty) should equal("WITH *")
  }

  test("renders regular projections") {
    format(QueryProjection(projections = Map("a" -> ident("b")))) should equal("WITH Identifier(\"b\") AS `a`")
  }

  test("renders skip") {
    format(QueryProjection(skip = Some(SignedIntegerLiteral("1")_))) should equal("WITH * SKIP SignedIntegerLiteral(\"1\")")
  }

  test("renders limit") {
    format(QueryProjection(limit = Some(SignedIntegerLiteral("1")_))) should equal("WITH * LIMIT SignedIntegerLiteral(\"1\")")
  }

  test("renders order by") {
    format(QueryProjection(sortItems = Seq(AscSortItem(ident("a"))_))) should equal("WITH * ORDER BY Identifier(\"a\")")
    format(QueryProjection(sortItems = Seq(DescSortItem(ident("a"))_))) should equal("WITH * ORDER BY Identifier(\"a\") DESC")
  }
}
