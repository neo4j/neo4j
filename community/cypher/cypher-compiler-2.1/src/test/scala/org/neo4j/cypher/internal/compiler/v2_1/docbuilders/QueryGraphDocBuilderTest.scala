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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.perty.{DocFormatters, condense, PrintNewLine, PrintText}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.{toStringDocBuilder, scalaDocBuilder, DocBuilderTestSuite}

class QueryGraphDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder =
      queryGraphDocBuilder orElse
      astExpressionDocBuilder orElse
      astDocBuilder orElse
      plannerDocBuilder orElse
      scalaDocBuilder orElse
      toStringDocBuilder

  private val rel1 = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(), SimplePatternLength)
  private val rel2 = PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.INCOMING, Seq(RelTypeName("X")(null)), SimplePatternLength)
  private val rel3 = PatternRelationship(IdName("r3"), (IdName("c"), IdName("d")), Direction.OUTGOING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(1, None))
  private val rel4 = PatternRelationship(IdName("r4"), (IdName("d"), IdName("c")), Direction.INCOMING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(0, Some(2)))

  test("render id names") {
    format(IdName("a")) should equal("a")
  }

  test("render rel type names") {
    format(RelTypeName("X")(null)) should equal("X")
  }

  test("render pattern rels") {
    format(rel1) should equal("(a)-[r1]->(b)")
    format(rel2) should equal("(b)<-[r2:X]-(a)")
    format(rel3) should equal("(c)-[r3:X|:Y*1..]->(d)")
    format(rel4) should equal("(d)<-[r4:X|:Y*0..2]-(c)")
  }

  test("render empty query graphs") {
    format(QueryGraph.empty) should equal("GIVEN *")
  }

  test("renders query graph arguments") {
    format(QueryGraph(argumentIds = Set(IdName("a")))) should equal("GIVEN a")
    format(QueryGraph(argumentIds = Set(IdName("a"), IdName("b")))) should equal("GIVEN a, b")
  }

  test("renders query graph nodes") {
    format(QueryGraph(patternNodes = Set(IdName("a"), IdName("b")))) should equal("GIVEN * MATCH (a), (b)")
  }

  test("renders query graph rels") {
    format(QueryGraph(
      patternNodes = Set(IdName("a"), IdName("b")),
      patternRelationships = Set(rel1)
    )) should equal("GIVEN * MATCH (a), (b), (a)-[r1]->(b)")
  }

  test("renders query graph shortest paths") {
    format(QueryGraph(
      patternNodes = Set(IdName("a")),
      shortestPathPatterns = Set(ShortestPathPattern(None, rel1, single = true)(null))
    )) should equal("GIVEN * MATCH (a), shortestPath((a)-[r1]->(b))")
  }

  test("renders query graph named all shortest paths") {
    format(QueryGraph(
      patternNodes = Set(IdName("a")),
      shortestPathPatterns = Set(ShortestPathPattern(Some(IdName("p")), rel1, single = false)(null))
    )) should equal("GIVEN * MATCH (a), p = allShortestPath((a)-[r1]->(b))")
  }

  test("renders query graph selections") {
    format(QueryGraph(
      patternNodes = Set(IdName("a")),
      selections = Selections( predicates = Set(Predicate(Set(IdName("a")), HasLabels(ident("a"), Seq(LabelName("Person")_))_)))
    )) should equal("GIVEN * MATCH (a) WHERE Predicate[a](a:Person)")
  }

  test("renders optional query graphs") {
    format(QueryGraph(
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(IdName("a")))
      )
    ))  should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a) }")
  }

  test("renders multiple optional query graphs") {
    format(QueryGraph(
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(IdName("a"))),
        QueryGraph(patternNodes = Set(IdName("b")))
      )
    )) should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a), GIVEN * MATCH (b) }")
  }

  test("renders hints") {
    val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Person")_, ident("name"))_

    format(QueryGraph(hints = Set(hint))) should equal("GIVEN * USING INDEX n:Person(name)")
  }

  test("indents sections correctly") {
    val result = condense(build(QueryGraph(
      patternNodes = Set(IdName("a"))
    ), formatter = DocFormatters.pageFormatter(8)))

    result should equal(Seq(
      PrintText("GIVEN *"),
      PrintNewLine(0),
      PrintText("MATCH"),
      PrintNewLine(2),
      PrintText("(a)")
    ))
  }
}
