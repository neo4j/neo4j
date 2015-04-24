/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class QueryTaggerTest extends CypherFunSuite {

  test("Tags match clauses with :match") {
    QueryTagger("MATCH n RETURN n") should contain(MatchTag)
  }

  test("Tags optional clauses with :opt") {
    QueryTagger("OPTIONAL MATCH n RETURN 1") should contain(OptionalTag)
  }

  test("Tags used expressions with :expr") {
    QueryTagger("RETURN n") should contain(ExpressionTag)
  }

  test("Tags filtering expressions with :filtering-expr") {
    QueryTagger("RETURN any(n in [1,2] where n > 0)") should contain(FilteringExpressionTag)
  }

  test("Supports combining tags") {
    QueryTagger("MATCH n RETURN n") should be(Set(MatchTag, ExpressionTag))
  }
}
