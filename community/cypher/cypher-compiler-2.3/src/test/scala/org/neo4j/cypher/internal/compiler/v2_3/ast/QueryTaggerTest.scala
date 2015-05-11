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

  test(queryTag(MatchTag)) {
    QueryTagger("MATCH n RETURN n") should contain(MatchTag)
  }

  test(queryTag(OptionalMatchTag)) {
    QueryTagger("OPTIONAL MATCH n RETURN 1") should contain(OptionalMatchTag)
  }

  test(queryTag(ShortestPathTag)) {
    QueryTagger("MATCH shortestPath( ()-[*]->() ) RETURN 1") should contain(ShortestPathTag)
  }

  test(queryTag(NamedPathTag)) {
    QueryTagger("MATCH p = ()-[*]->() RETURN 1") should contain(NamedPathTag)
  }

  test(queryTag(SingleLengthRelTag)) {
    QueryTagger("MATCH ()-[]->() RETURN 1") should contain(SingleLengthRelTag)
  }

  test(queryTag(VarLengthRelTag)) {
    QueryTagger("MATCH ()-[*]->() RETURN 1") should contain(VarLengthRelTag)
  }

  test(queryTag(DirectedRelTag)) {
    QueryTagger("MATCH ()-[]->() RETURN 1") should contain(DirectedRelTag)
  }

  test(queryTag(UnDirectedRelTag)) {
    QueryTagger("MATCH ()-[]-() RETURN 1") should contain(UnDirectedRelTag)
  }

  test(queryTag(RelPatternTag)) {
    QueryTagger("MATCH ()-[]->() RETURN 1") should contain(RelPatternTag)
  }

  test(queryTag(SingleNodePatternTag)) {
    QueryTagger("MATCH () RETURN 1") should contain(SingleNodePatternTag)
    QueryTagger("MATCH ()-[]->() RETURN 1") should not contain SingleNodePatternTag
  }

  test(queryTag(ComplexExpressionTag)) {
    QueryTagger("RETURN n + 1") should contain(ComplexExpressionTag)
  }

  test(queryTag(FilteringExpressionTag)) {
    QueryTagger("RETURN any(n in [1,2] where n > 0)") should contain(FilteringExpressionTag)
  }

  test("Supports combining tags") {
    QueryTagger("MATCH n RETURN n") should be(Set(
      MatchTag,
      RegularMatchTag,
      SingleNodePatternTag,
      IdentifierExpressionTag)
    )
  }

  private def queryTag(tag: QueryTag) = tag.toString
}
