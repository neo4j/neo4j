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

import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.{queryProjectionDocBuilder, queryHorizonDocBuilder, astExpressionDocBuilder, queryShuffleDocBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryHorizon

class QueryHorizonDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder =
    queryHorizonDocBuilder orElse
    queryProjectionDocBuilder("WITH") orElse
    queryShuffleDocBuilder orElse
    astExpressionDocBuilder orElse
    scalaDocBuilder orElse
    toStringDocBuilder

  test("Empty query horizon") {
    format(QueryHorizon()) should equal("WITH *")
  }

  test("Query horizon with single unwind") {
    format(QueryHorizon(unwinds = Map("a" -> ident("b")))) should equal("UNWIND b AS `a` WITH *")
  }

  test("Query horizon with many unwinds") {
    format(QueryHorizon(unwinds = Map("a" -> ident("b"), "x" -> ident("z")))) should equal("UNWIND b AS `a` UNWIND z AS `x` WITH *")
  }
}
