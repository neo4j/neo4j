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

import org.neo4j.cypher.internal.compiler.v2_1.ast.{CountStar, SignedDecimalIntegerLiteral, DescSortItem, AscSortItem}
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.{scalaDocBuilder, toStringDocBuilder, DocBuilderTestSuite}
import org.neo4j.cypher.internal.compiler.v2_1.perty.DocBuilder
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName

class QueryProjectionDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder: DocBuilder[Any] =
    astExpressionDocBuilder orElse
    queryProjectionDocBuilder("WITH") orElse
    queryShuffleDocBuilder orElse
    plannerDocBuilder orElse
    scalaDocBuilder orElse
    toStringDocBuilder

  test("calls down to astExpressionDocBuilder") {
    format(ident("a")) should equal("a")
  }

  test("renders star projections") {
    format(QueryProjection.empty) should equal("WITH *")
  }

  test("renders regular projections") {
    format(RegularQueryProjection(projections = Map("a" -> ident("b")))) should equal("WITH b AS `a`")
  }

  test("renders aggregating projections") {
    format(AggregatingQueryProjection(
      groupingKeys = Map("a" -> ident("b")),
      aggregationExpressions = Map("x" -> CountStar()_)
    )) should equal("WITH b AS `a`, count(*) AS `x`")
  }

  test("renders skip") {
    format(RegularQueryProjection(shuffle = QueryShuffle(skip = Some(SignedDecimalIntegerLiteral("1")_)))) should equal("WITH * SKIP 1")
  }

  test("renders limit") {
    format(RegularQueryProjection(shuffle = QueryShuffle(limit = Some(SignedDecimalIntegerLiteral("1")_)))) should equal("WITH * LIMIT 1")
  }

  test("renders order by") {
    format(RegularQueryProjection(shuffle = QueryShuffle(sortItems = Seq(AscSortItem(ident("a"))_)))) should equal("WITH * ORDER BY a")
    format(RegularQueryProjection(shuffle = QueryShuffle(sortItems = Seq(DescSortItem(ident("a"))_)))) should equal("WITH * ORDER BY a DESC")
  }

  test("renders unwind") {
    format(UnwindProjection(identifier = IdName("name"), ident("n"))) should equal("UNWIND n AS `name`")
  }
}
