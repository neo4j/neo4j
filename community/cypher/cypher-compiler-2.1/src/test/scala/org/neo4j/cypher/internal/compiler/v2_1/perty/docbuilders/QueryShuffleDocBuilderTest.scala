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
package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.{astExpressionDocBuilder, queryShuffleDocBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryShuffle
import org.neo4j.cypher.internal.compiler.v2_1.ast.{SignedDecimalIntegerLiteral, DescSortItem, AscSortItem}

class QueryShuffleDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder =
    queryShuffleDocBuilder orElse
    astExpressionDocBuilder orElse
    scalaDocBuilder orElse
    toStringDocBuilder

  test("Empty query shuffle") {
    format(QueryShuffle()) should equal("")
  }

  test("ORDER BY item") {
    format(QueryShuffle(Seq(AscSortItem(ident("item"))_))) should equal("ORDER BY item")
  }

  test("ORDER BY item DEC") {
    format(QueryShuffle(Seq(DescSortItem(ident("item"))_))) should equal("ORDER BY item DESC")
  }

  test("SKIP 5") {
    format(QueryShuffle(Seq.empty, skip = Some(SignedDecimalIntegerLiteral("5")_))) should equal("SKIP 5")
  }

  test("LIMIT 5") {
    format(QueryShuffle(Seq.empty, limit = Some(SignedDecimalIntegerLiteral("5")_))) should equal("LIMIT 5")
  }

  test("ORDER BY item1, item2 DESC SKIP 5 LIMIT 5") {
    format(QueryShuffle(
      Seq(AscSortItem(ident("item1"))_, DescSortItem(ident("item2"))_),
      skip = Some(SignedDecimalIntegerLiteral("5")_),
      limit = Some(SignedDecimalIntegerLiteral("5")_)
    )) should equal("ORDER BY item1, item2 DESC SKIP 5 LIMIT 5")
  }
}
