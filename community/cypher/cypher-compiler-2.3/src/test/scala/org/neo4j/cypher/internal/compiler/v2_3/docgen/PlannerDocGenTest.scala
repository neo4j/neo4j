/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty.DocFormatters
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.DocHandlerTestSuite
import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.DefaultDocHandler
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintNewLine, PrintText, condense}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}

class PlannerDocGenTest extends DocHandlerTestSuite[Any] with AstConstructionTestSupport {

  val docGen =
    plannerDocGen orElse
    AstDocHandler.docGen.lift[Any] orElse
    DefaultDocHandler.docGen

  val rel1 = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), OUTGOING, Seq(), SimplePatternLength)
  val rel2 = PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), INCOMING, Seq(RelTypeName("X")(null)), SimplePatternLength)
  val rel3 = PatternRelationship(IdName("r3"), (IdName("c"), IdName("d")), OUTGOING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(1, None))
  val rel4 = PatternRelationship(IdName("r4"), (IdName("d"), IdName("c")), INCOMING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(0, Some(2)))

  test("IdName(a) => a") {
    pprintToString(IdName("a")) should equal("a")
  }

  test("Predicate[a,b](True) => Predicate[a,b](true)") {
    pprintToString(Predicate(Set(IdName("a"), IdName("b")), True()_)) should equal("Predicate[a,b](true)")
  }

  test("Selections(Predicate[a](True), Predicate[b](False)) => Predicate[a](true), Predicate[b](false)") {
    val phrase = Selections(Set(
      Predicate(Set(IdName("a")), True()_),
      Predicate(Set(IdName("b")), False()_)
    ))

    val result = pprintToString(phrase)

    result should equal("Predicate[a](true), Predicate[b](false)")
  }

  test("SimplePatternLength => empty string") {
    val phrase = SimplePatternLength

    val result = pprintToString(phrase)

    result should equal("")
  }

  test("VarPatternLength(1,*) => *1..") {
    val phrase = VarPatternLength(1, None)

    val result = pprintToString(phrase)

    result should equal("*1..")
  }

  test("VarPatternLength(1,2) => *1..2") {
    val phrase = VarPatternLength(1, Some(2))

    val result = pprintToString(phrase)

    result should equal("*1..2")
  }

  test("PatternRelationship(rel, (a, b), ->, Seq(), SimplePatternLength) => (a)-[rel]->(b)") {
    val phrase = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength)

    val result = pprintToString(phrase)

    result should equal("(a)-[rel]->(b)")
  }

  test("PatternRelationship(rel, (a, b), <-, Seq(), SimplePatternLength) => (a)<-[rel]-(b)") {
    val phrase = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, SimplePatternLength)

    val result = pprintToString(phrase)

    result should equal("(a)<-[rel]-(b)")
  }


  test("PatternRelationship(rel, (a, b), <->, Seq(), SimplePatternLength) => (a)-[rel]-(b)") {
    val phrase = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), BOTH, Seq.empty, SimplePatternLength)

    val result = pprintToString(phrase)

    result should equal("(a)-[rel]-(b)")
  }

  test("PatternRelationship(rel, (a, b), ->, [R], SimplePatternLength) => (a)-[rel:R]-(b)") {
    val phrase = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING,
      Seq(RelTypeName("R")_), SimplePatternLength)

    val result = pprintToString(phrase)

    result should equal("(a)-[rel:R]->(b)")
  }

  test("PatternRelationship(rel, (a, b), ->, [R1,R2], SimplePatternLength) => (a)-[rel:R1|R2]-(b)") {
    val phrase = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING,
      Seq(RelTypeName("R1")_, RelTypeName("R2")_), SimplePatternLength)

    val result = pprintToString(phrase)

    result should equal("(a)-[rel:R1|R2]->(b)")
  }

  test("sp = shortestPath((a)-[rel*1..]->(b))") {
    val patRel = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, VarPatternLength(1, None))
    val nodeA = NodePattern(identifier = Some(ident("a")), Seq.empty, None, naked = false)_
    val nodeB = NodePattern(identifier = Some(ident("b")), Seq.empty, None, naked = false)_
    val length: Some[Some[Range]] = Some(Some(Range(Some(UnsignedDecimalIntegerLiteral("1")_), None)_))

    val relPat = RelationshipPattern(Some(ident("rel")), optional = false, Seq.empty, length, None, OUTGOING)_
    val ast = ShortestPaths(RelationshipChain(nodeA, relPat, nodeB)_, single = true)_
    val sp = ShortestPathPattern(Some(IdName("sp")), patRel, single = true)(ast)

    val result = pprintToString(sp)

    result should equal("sp = shortestPath((a)-[rel*1..]->(b))")
  }

  test("shortestPath((a)-[rel*1..]->(b))") {
    val patRel = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, VarPatternLength(1, None))
    val nodeA = NodePattern(identifier = Some(ident("a")), Seq.empty, None, naked = false)_
    val nodeB = NodePattern(identifier = Some(ident("b")), Seq.empty, None, naked = false)_
    val length: Some[Some[Range]] = Some(Some(Range(Some(UnsignedDecimalIntegerLiteral("1")_), None)_))

    val relPat = RelationshipPattern(Some(ident("rel")), optional = false, Seq.empty, length, None, OUTGOING)_
    val ast = ShortestPaths(RelationshipChain(nodeA, relPat, nodeB)_, single = true)_
    val sp = ShortestPathPattern(None, patRel, single = true)(ast)

    val result = pprintToString(sp)

    result should equal("shortestPath((a)-[rel*1..]->(b))")
  }

  test("sp = allShortestPath((a)-[rel*1..]->(b))") {
    val patRel = PatternRelationship(IdName("rel"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, VarPatternLength(1, None))
    val nodeA = NodePattern(identifier = Some(ident("a")), Seq.empty, None, naked = false)_
    val nodeB = NodePattern(identifier = Some(ident("b")), Seq.empty, None, naked = false)_
    val length: Some[Some[Range]] = Some(Some(Range(Some(UnsignedDecimalIntegerLiteral("1")_), None)_))

    val relPat = RelationshipPattern(Some(ident("rel")), optional = false, Seq.empty, length, None, OUTGOING)_
    val ast = ShortestPaths(RelationshipChain(nodeA, relPat, nodeB)_, single = false)_
    val sp = ShortestPathPattern(Some(IdName("sp")), patRel, single = false)(ast)

    val result = pprintToString(sp)

    result should equal("sp = allShortestPath((a)-[rel*1..]->(b))")
  }


  test("Empty query shuffle") {
    pprintToString(QueryShuffle()) should equal("")
  }

  test("ORDER BY item") {
    pprintToString(QueryShuffle(Seq(AscSortItem(ident("item")) _))) should equal("ORDER BY item")
  }

  test("ORDER BY item DESC") {
    pprintToString(QueryShuffle(Seq(DescSortItem(ident("item")) _))) should equal("ORDER BY item DESC")
  }

  test("SKIP 5") {
    pprintToString(QueryShuffle(Seq.empty, skip = Some(SignedDecimalIntegerLiteral("5") _))) should equal("SKIP 5")
  }

  test("LIMIT 5") {
    pprintToString(QueryShuffle(Seq.empty, limit = Some(SignedDecimalIntegerLiteral("5") _))) should equal("LIMIT 5")
  }

  test("ORDER BY item1, item2 DESC SKIP 5 LIMIT 5") {
    pprintToString(QueryShuffle(
      Seq(AscSortItem(ident("item1")) _, DescSortItem(ident("item2")) _),
      skip = Some(SignedDecimalIntegerLiteral("5") _),
      limit = Some(SignedDecimalIntegerLiteral("5") _)
    )) should equal("ORDER BY item1, item2 DESC SKIP 5 LIMIT 5")
  }

    test("render id names") {
      pprintToString(IdName("a")) should equal("a")
    }

    test("render rel type names") {
      pprintToString(RelTypeName("X")(null)) should equal("X")
    }

    test("render pattern rels") {
      pprintToString(rel1) should equal("(a)-[r1]->(b)")
      pprintToString(rel2) should equal("(b)<-[r2:X]-(a)")
      pprintToString(rel3) should equal("(c)-[r3:X|Y*1..]->(d)")
      pprintToString(rel4) should equal("(d)<-[r4:X|Y*0..2]-(c)")
    }

    test("render empty query graphs") {
      pprintToString(QueryGraph.empty) should equal("GIVEN *")
    }

    test("renders query graph arguments") {
      pprintToString(QueryGraph(argumentIds = Set(IdName("a")))) should equal("GIVEN a")
      pprintToString(QueryGraph(argumentIds = Set(IdName("a"), IdName("b")))) should equal("GIVEN a, b")
    }

    test("renders query graph nodes") {
      pprintToString(QueryGraph(patternNodes = Set(IdName("a"), IdName("b")))) should equal("GIVEN * MATCH (a), (b)")
    }

    test("renders query graph rels") {
      pprintToString(QueryGraph(
        patternNodes = Set(IdName("a"), IdName("b")),
        patternRelationships = Set(rel1)
      )) should equal("GIVEN * MATCH (a), (b), (a)-[r1]->(b)")
    }

    test("renders query graph shortest paths") {
      pprintToString(QueryGraph(
        patternNodes = Set(IdName("a")),
        shortestPathPatterns = Set(ShortestPathPattern(None, rel1, single = true)(null))
      )) should equal("GIVEN * MATCH (a), shortestPath((a)-[r1]->(b))")
    }

    test("renders query graph named all shortest paths") {
      pprintToString(QueryGraph(
        patternNodes = Set(IdName("a")),
        shortestPathPatterns = Set(ShortestPathPattern(Some(IdName("p")), rel1, single = false)(null))
      )) should equal("GIVEN * MATCH (a), p = allShortestPath((a)-[r1]->(b))")
    }

    test("renders query graph selections") {
      pprintToString(QueryGraph(
        patternNodes = Set(IdName("a")),
        selections = Selections( predicates = Set(Predicate(Set(IdName("a")), HasLabels(ident("a"), Seq(LabelName("Person")_))_)))
      )) should equal("GIVEN * MATCH (a) WHERE Predicate[a](a:Person)")
    }

    test("renders optional query graphs") {
      pprintToString(QueryGraph(
        optionalMatches = Seq(
          QueryGraph(patternNodes = Set(IdName("a")))
        )
      ))  should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a) }")
    }

    test("renders multiple optional query graphs") {
      pprintToString(QueryGraph(
        optionalMatches = Seq(
          QueryGraph(patternNodes = Set(IdName("a"))),
          QueryGraph(patternNodes = Set(IdName("b")))
        )
      )) should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a), GIVEN * MATCH (b) }")
    }

    test("renders hints") {
      val hint: UsingIndexHint = UsingIndexHint(ident("n"), LabelName("Person")_, PropertyKeyName("name")(pos))_

      pprintToString(QueryGraph(hints = Set(hint))) should equal("GIVEN * USING INDEX n:Person(name)")
    }

    test("indents sections correctly") {
      val result = condense(DocFormatters.pageFormatter(8)(convert(QueryGraph(patternNodes = Set(IdName("a"))))))

      result should equal(Seq(
        PrintText("GIVEN *"),
        PrintNewLine(0),
        PrintText("MATCH"),
        PrintNewLine(2),
        PrintText("(a)")
      ))
    }

    test("calls down to astExpressionDocGen") {
      pprintToString(ident("a")) should equal("a")
    }

    test("renders star projections") {
      pprintToString(QueryProjection.empty) should equal("*")
    }

    test("renders regular projections") {
      pprintToString(RegularQueryProjection(projections = Map("a" -> ident("b")))) should equal("b AS `a`")
    }

    test("renders aggregating projections") {
      pprintToString(AggregatingQueryProjection(
        groupingKeys = Map("a" -> ident("b")),
        aggregationExpressions = Map("x" -> CountStar()_)
      )) should equal("b AS `a`, count(*) AS `x`")
    }

    test("renders skip") {
      pprintToString(RegularQueryProjection(shuffle = QueryShuffle(skip = Some(SignedDecimalIntegerLiteral("1")_)))) should equal("* SKIP 1")
    }

    test("renders limit") {
      pprintToString(RegularQueryProjection(shuffle = QueryShuffle(limit = Some(SignedDecimalIntegerLiteral("1")_)))) should equal("* LIMIT 1")
    }

    test("renders order by") {
      pprintToString(RegularQueryProjection(shuffle = QueryShuffle(sortItems = Seq(AscSortItem(ident("a"))_)))) should equal("* ORDER BY a")
      pprintToString(RegularQueryProjection(shuffle = QueryShuffle(sortItems = Seq(DescSortItem(ident("a"))_)))) should equal("* ORDER BY a DESC")
    }

    test("renders unwind") {
      pprintToString(UnwindProjection(identifier = IdName("name"), ident("n"))) should equal("UNWIND n AS `name`")
    }

    test("renders tail free empty planner query") {
      pprintToString(PlannerQuery(
        graph = QueryGraph(),
        horizon = QueryProjection.empty
      )) should equal("GIVEN * RETURN *")
    }

    test("renders tail free non-empty planner query") {
      pprintToString(PlannerQuery(
        graph = QueryGraph(patternNodes = Set(IdName("a"))),
        horizon = RegularQueryProjection(projections = Map("a" -> SignedDecimalIntegerLiteral("1") _))
      )) should equal("GIVEN * MATCH (a) RETURN 1 AS `a`")
    }

    test("render planner query with tail") {
      pprintToString(PlannerQuery(
        graph = QueryGraph(patternNodes = Set(IdName("a"))),
        horizon = RegularQueryProjection(projections = Map("a" -> SignedDecimalIntegerLiteral("1") _)),
        tail = Some(PlannerQuery.empty)
      )) should equal("GIVEN * MATCH (a) WITH 1 AS `a` GIVEN * RETURN *")
    }
}
