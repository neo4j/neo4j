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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.planner._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class PlannerQueryRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val rewriter = PlannerQueryRewriter

  test("empty query stays empty") {
    val empty = RegularPlannerQuery()
    empty.endoRewrite(rewriter) should equal(empty)
  }

  test("unused optional match is removed") {
    /*
        MATCH (a)
        OPTIONAL MATCH (a)-->(b)
        RETURN distinct a

        is equivalent to:

        MATCH (a)
        RETURN distinct a
    */

    val original = {
      val optionalGraph = QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(
          PatternRelationship(IdName("r"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING,
            types = Seq.empty, length = SimplePatternLength)))
      val qg = QueryGraph(patternNodes = Set(IdName("a")))
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a")), aggregationExpressions = Map.empty)
      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
    }

    val expected = {
      val qg = QueryGraph(patternNodes = Set(IdName("a")), optionalMatches = Vector.empty)
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a")))
      RegularPlannerQuery(queryGraph = qg, horizon = projection)
    }

    original.endoRewrite(rewriter) should equal(expected)
  }

  test("used optional match is not removed") {
    /*
        MATCH (a)
        OPTIONAL MATCH (a)-->(b)
        RETURN DISTINCT a, b

        is equivalent to:

        MATCH (a)
        OPTIONAL MATCH (b) WHERE (a)-->(b)
        RETURN DISTINCT a, b
    */

    val original = {
      val optionalGraph = QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(
          PatternRelationship(IdName("r"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING,
            types = Seq.empty, length = SimplePatternLength)))
      val qg = QueryGraph(patternNodes = Set(IdName("a")))
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("b")), aggregationExpressions = Map.empty)
      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
    }

    original.endoRewrite(rewriter) should equal(original)
  }

  test("unused optional pattern relationship not kept around") {
    /*
        MATCH (a), (b)
        OPTIONAL MATCH (a)-->(b)
        RETURN DISTINCT a, b

        is equivalent to:

        MATCH (a), (b)
        RETURN DISTINCT a, b
    */

    val original = {
      val optionalGraph = QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(
          PatternRelationship(IdName("r"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING,
            types = Seq.empty, length = SimplePatternLength)))
      val qg = QueryGraph(patternNodes = Set(IdName("a"), IdName("b")))
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("b")), aggregationExpressions = Map.empty)
      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
    }

    val expected = {
      val qg = QueryGraph(patternNodes = Set(IdName("a"), IdName("b")), optionalMatches = Vector.empty)
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("b")))
      RegularPlannerQuery(queryGraph = qg, horizon = projection)
    }


    original.endoRewrite(rewriter) should equal(expected)
  }

  test("returning a relationship introduced in an optional match") {
    /*
        MATCH (a)
        OPTIONAL MATCH (a)-[r1]->(b)
        RETURN DISTINCT a, r1

        cannot be simplified
    */

    val original = {
      val optionalGraph = QueryGraph(
        patternNodes = Set(IdName("a")),
        patternRelationships = Set(
          PatternRelationship(IdName("r"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING,
            types = Seq.empty, length = SimplePatternLength)))
      val qg = QueryGraph(patternNodes = Set(IdName("a")))
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("r")), aggregationExpressions = Map.empty)
      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
    }

    original.endoRewrite(rewriter) should equal(original)
  }

  ignore("unused optional relationship moved to predicate") { // not done
    /*
        MATCH (a)
        OPTIONAL MATCH (a)-->(b)-->(c)
        RETURN DISTINCT a, b

        is equivalent to:

        MATCH (a)
        OPTIONAL MATCH (a)-->(b) WHERE (b)-->(c)
        RETURN DISTINCT a, b
    */

    val original = {
      val optionalGraph = QueryGraph(
        patternNodes = Set(IdName("a")),
        patternRelationships = Set(
          PatternRelationship(IdName("r1"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING, types = Seq.empty, length = SimplePatternLength),
          PatternRelationship(IdName("r2"), nodes = (IdName("b"), IdName("c")), dir = OUTGOING, types = Seq.empty, length = SimplePatternLength)
        ))
      val qg = QueryGraph(patternNodes = Set(IdName("a")))
      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("r")), aggregationExpressions = Map.empty)
      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
    }

//    val expected = {
//      val optionalGraph = QueryGraph(
//        patternNodes = Set(IdName("a")),
//        patternRelationships = Set(
//          PatternRelationship(IdName("r1"), nodes = (IdName("a"), IdName("b")), dir = OUTGOING, types = Seq.empty, length = SimplePatternLength)
//        ),
//        selections = Selections.from(PatternExpression())
//      )
//      val qg = QueryGraph(patternNodes = Set(IdName("a")))
//      val projection = AggregatingQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("r")), aggregationExpressions = Map.empty)
//      RegularPlannerQuery(queryGraph = qg.withAddedOptionalMatch(optionalGraph), horizon = projection)
//    }

    original.endoRewrite(rewriter) should equal(original)
  }

}
