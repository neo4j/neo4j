/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v4_0.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v4_0.planner._
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.v4_0.logical.plans.FieldSignature
import org.neo4j.cypher.internal.v4_0.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.v4_0.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.v4_0.logical.plans.QualifiedName
import org.neo4j.cypher.internal.v4_0.ast.Hint
import org.neo4j.cypher.internal.v4_0.ast.UsingIndexHint
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.helpers.StringHelper._
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class StatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val patternRel = PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength)
  private val nProp = prop("n", "prop")

  test("RETURN 42") {
    val query = buildPlannerQuery("RETURN 42")
    query.horizon should equal(RegularQueryProjection(Map("42" -> literalInt(42))))
  }

  test("RETURN 42, 'foo'") {
    val query = buildPlannerQuery("RETURN 42, 'foo'")
    query.horizon should equal(RegularQueryProjection(Map(
      "42" -> literalInt(42),
      "'foo'" -> literalString("foo")
    )))
  }

  test("match (n) return n") {
    val query = buildPlannerQuery("match (n) return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:A:B RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:A:B RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A")),
      Predicate(Set("n"), hasLabels("n", "B")
      ))))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) where n:X OR n:Y return n") {
    val query = buildPlannerQuery("match (n) where n:X OR n:Y return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), ors(
        hasLabels("n", "X"),
        hasLabels("n", "Y")
      )
      ))))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), ors(
        hasLabels("n", "X"),
        hasLabels("n", "B")
      )),
      Predicate(Set("n"), ors(
        hasLabels("n", "X"),
        hasLabels("n", "A")
      ))
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE id(n) = 42 RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), in(
        id(varFor("n")),
        listOfInt(42)
      )
      ))))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE id(n) IN [42, 43] RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE id(n) IN [42, 43] RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), in(
        id(varFor("n")),
        listOfInt(42, 43)
      )
      ))))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> varFor("n")
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A")),
      Predicate(Set("n"), in(
        id(varFor("n")),
        listOfInt(42)
      )
      ))))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (p) = (a) return p") {
    val query = buildPlannerQuery("match p = (a) return p")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set[String]("a"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression](
      "p" -> PathExpression(NodePathStep(varFor("a"), NilPathStep))_
    )))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val query = buildPlannerQuery("match p = (a)-[r]->(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(patternRel))
    query.queryGraph.patternNodes should equal(Set[String]("a", "b"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b, c") {
    val query = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship("r2", ("b", "c"), OUTGOING, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    val predicate = Predicate(Set("r", "r2"), not(equals(varFor("r"), varFor("r2"))))
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r"),
      "b" -> varFor("b")
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val query = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(a) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship("r2", ("b", "a"), OUTGOING, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    val predicate = Predicate(Set("r", "r2"), not(equals(varFor("r"), varFor("r2"))))
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship("r2", ("b", "c"), BOTH, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    val predicate = Predicate(Set("r", "r2"), not(equals(varFor("r"), varFor("r2"))))
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship("r2", ("b", "c"), BOTH, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    val predicate = Predicate(Set("r", "r2"), not(equals(varFor("r"), varFor("r2"))))
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val query = buildPlannerQuery("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("n", "c"), BOTH, Seq(relType("Type")), SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "n", "c"))
    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A"))
    )))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type|Foo]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c") {
    val query = buildPlannerQuery("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r1", ("a", "b"), OUTGOING, Seq(relType("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship("r2", ("b", "c"), OUTGOING, Seq(relType("FRIEND")), VarPatternLength(0, Some(1)))))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "b" -> varFor("b"),
      "c" -> varFor("c")
    )))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*3..]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*5]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship("r2", ("b", "c"), BOTH, Seq.empty, VarPatternLength(1, None))))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))

    val inner = anyInList("r2", varFor("r2"), equals(varFor("r"), varFor("r2")))
    val outer = noneInList("r", varFor("r"), inner)
    val predicate = Predicate(Set("r2", "r"), outer)

    query.queryGraph.selections should equal(Selections(Set(predicate)))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "r" -> varFor("r")
    )))
  }

  test("optional match (a) return a") {
    val query = buildPlannerQuery("optional match (a) return a")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a")
    )))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set("a"))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("optional match (a)-[r]->(b) return a,b,r") {
    val query = buildPlannerQuery("optional match (a)-[r]->(b) return a,b,r")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "b" -> varFor("b"),
      "r" -> varFor("r")
    )))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())


    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set("a", "b"))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be (empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match (a) optional match (a)-[r]->(b) return a,b,r") {
    val query = buildPlannerQuery("match (a) optional match (a)-[r]->(b) return a,b,r")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> varFor("a"),
      "b" -> varFor("b"),
      "r" -> varFor("r")
    )))
    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternNodes should equal(Set("a", "b"))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set("a"))
  }

  test("match (a) where (a)-->() return a") {
    // Given
    val query = buildPlannerQuery("match (a) where (a)-->() return a")

    // Then inner pattern query graph
    val exp = greaterThan(GetDegree(varFor("a"),None,OUTGOING)_, literalInt(0))
    val predicate= Predicate(Set("a"), exp)
    val selections = Selections(Set(predicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("MATCH (a) WITH 1 AS b RETURN b") {
    val query = buildPlannerQuery("MATCH (a) WITH 1 AS b RETURN b")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(RegularQueryProjection(Map("b" -> literalInt(1))))
    query.tail should equal(Some(RegularPlannerQuery(QueryGraph(Set.empty, Set.empty, Set("b")), InterestingOrder.empty,
      RegularQueryProjection(Map("b" -> varFor("b"))))))
  }

  test("WITH 1 AS b RETURN b") {
    val query = buildPlannerQuery("WITH 1 AS b RETURN b")

    query.horizon should equal(RegularQueryProjection(Map("b" -> literalInt(1))))
    query.tail should equal(Some(RegularPlannerQuery(QueryGraph(Set.empty, Set.empty, Set("b")), InterestingOrder.empty,
      RegularQueryProjection(Map("b" -> varFor("b"))))))
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val result = buildPlannerQuery("MATCH (a) WITH a WHERE TRUE RETURN a")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("a"), selections = Selections(Set(Predicate(Set.empty, trueLiteral)))),
      horizon = RegularQueryProjection(projections = Map("a" -> varFor("a")))
    )

    result should equal(expectation)
  }

  test("match (a) where a.prop = 42 OR (a)-->() return a") {
    // Given
    val query = buildPlannerQuery("match (a) where a.prop = 42 OR (a)-->() return a")

    // Then inner pattern query graph
    val exp1 = greaterThan(GetDegree(varFor("a"),None,OUTGOING)_, literalInt(0))
    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp2))
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (a) where (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildPlannerQuery("match (a) where (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val exp1 = greaterThan(GetDegree(varFor("a"),None,OUTGOING)_, literalInt(0))
    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp2))
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildPlannerQuery("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val exp1 = greaterThan(GetDegree(varFor("a"),None,OUTGOING)_, literalInt(0))
    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val exp3 = in(prop("a", "prop2"), listOfInt(21))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp3, exp2))

    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (n) return n limit 10") {
    // Given
    val query = buildPlannerQuery("match (n) return n limit 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set("n"))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->varFor("n")),
        QueryPagination(
          skip = None,
          limit = Some(literalInt(10)))))
  }

  test("match (n) return n skip 10") {
    // Given
    val query = buildPlannerQuery("match (n) return n skip 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set("n"))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->varFor("n")),
        QueryPagination(
          skip = Some(literalInt(10)),
          limit = None)))
  }

  test("match (a) with * return a") {
    val query = buildPlannerQuery("match (a) with * return a")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression]("a" -> varFor("a"))))
    query.tail should equal(None)
  }

  test("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b") {
    val query = buildPlannerQuery("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b")
    query.queryGraph should equal(
      QueryGraph
        .empty
        .addPatternNodes("a")
    )

    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("a" -> varFor("a")),
        QueryPagination(
          skip = None,
          limit = Some(literalInt(1)))))

    query.tail should not be empty
    query.tail.get.queryGraph should equal(
      QueryGraph
        .empty
        .addArgumentIds(Seq("a"))
        .addPatternNodes("a", "b")
        .addPatternRelationship(patternRel)
    )
  }

  test("optional match (a:Foo) with a match (a)-[r]->(b) return a") {
    val query = buildPlannerQuery("optional match (a:Foo) with a match (a)-[r]->(b) return a")

    query.queryGraph should equal(
      QueryGraph
        .empty
        .withAddedOptionalMatch(
          QueryGraph
            .empty
            .addPatternNodes("a")
            .addSelections(Selections(Set(Predicate(Set("a"), hasLabels("a", "Foo")))))
        ))
    query.horizon should equal(RegularQueryProjection(Map("a" -> varFor("a"))))
    query.tail should not be empty
    query.tail.get.queryGraph should equal(
      QueryGraph
        .empty
        .addArgumentIds(Seq("a"))
        .addPatternNodes("a", "b")
        .addPatternRelationship(patternRel)
    )
  }

  test("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b")
    query.tail should not be empty
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> prop("a", "prop")),
        QueryPagination(None, Some(literalInt(1)))
      )
    )
    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set("b"))
    tailQg.queryGraph.patternRelationships should be(empty)

    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryPagination()))
  }

  test("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))

    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> prop("a", "prop"))))

    val secondQuery = query.tail.get
    secondQuery.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )))

    secondQuery.horizon should equal(
      RegularQueryProjection(
        Map("b" -> varFor("b"))))
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b")
    query.tail should not be empty
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(AggregatingQueryProjection(
      Map("property" -> prop("a", "prop")),
      Map("count" -> CountStar()_)
    ))

    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set("b"))
    tailQg.queryGraph.patternRelationships should be(empty)
    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryPagination()))
  }

  test("MATCH (n) RETURN count(*)") {
    val query = buildPlannerQuery("MATCH (n) RETURN count(*)")

    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregationExpression, QueryPagination(limit, skip), where) =>
        groupingKeys should be (empty)
        limit should be (empty)
        skip should be (empty)
        where should be (empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be (empty)
    query.queryGraph.patternRelationships should be (empty)
    query.queryGraph.patternNodes should be (Set("n"))
  }

  test("MATCH (n) RETURN n.prop, count(*)") {
    val query = buildPlannerQuery("MATCH (n) RETURN n.prop, count(*)")

    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregationExpression, QueryPagination(limit, skip), where) =>
        groupingKeys should equal(Map("n.prop" -> nProp))
        limit should be (empty)
        skip should be (empty)
        where should be (empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be (empty)
    query.queryGraph.patternRelationships should be (empty)
    query.queryGraph.patternNodes should be (Set("n"))
  }

  test("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n") {
    val query = buildPlannerQuery("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n")

    query.queryGraph.hints should equal(Seq[Hint](UsingIndexHint(varFor("n"), labelName("Awesome"), Seq(PropertyKeyName("prop")(pos)))_))
  }

  test("MATCH shortestPath((a)-[r]->(b)) RETURN r") {
    val query = buildPlannerQuery("MATCH shortestPath((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some("  FRESHID6"), PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH allShortestPaths((a)-[r]->(b)) RETURN r") {
    val query = buildPlannerQuery("MATCH allShortestPaths((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some("  FRESHID6"), PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength), single = false)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH p = shortestPath((a)-[r]->(b)) RETURN p") {
    val query = buildPlannerQuery("MATCH p = shortestPath((a)-[r]->(b)) RETURN p")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some("p"), PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("match (n) return distinct n") {
    val query = buildPlannerQuery("match (n) return distinct n")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) with distinct n.prop as x return x") {
    val query = buildPlannerQuery("match (n) with distinct n.prop as x return x")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("x" -> nProp)
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) with distinct * return n") {
    val query = buildPlannerQuery("match (n) with distinct * return n")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("WITH DISTINCT 1 as b RETURN b") {
    val query = buildPlannerQuery("WITH DISTINCT 1 as b RETURN b")

    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("b" -> literalInt(1))
    ))

    query.tail should not be empty
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner") {
    val result = buildPlannerQuery("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner")

    // (owner)-[`  REL62`]-(`  NODE64`)
    val patternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("owner")), Seq.empty, None)(pos),
      RelationshipPattern(Some(varFor("  REL62")), Seq.empty, None, None, BOTH)(pos),
      NodePattern(Some(varFor("  NODE64")), Seq.empty, None)(pos))(pos))(pos))

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("owner")),
      horizon = AggregatingQueryProjection(
        groupingExpressions = Map("owner" -> varFor("owner")),
        aggregationExpressions = Map("collected" -> CountStar()(pos)),
        selections = Selections(Set(Predicate(Set("owner", "  REL62", "  NODE64"), patternExpression)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("collected", "owner")),
        horizon = RegularQueryProjection(projections = Map("owner" -> varFor("owner")))
      ))
    )

    result should equal(expectation)
  }

  test("Funny query from boostingRecommendations") {
    val query = buildPlannerQuery(
      """MATCH (origin)-[r1:KNOWS|WORKS_AT]-(c)-[r2:KNOWS|WORKS_AT]-(candidate)
        |WHERE origin.name = "Clark Kent"
        |AND type(r1)=type(r2) AND NOT (origin)-[:KNOWS]-(candidate)
        |RETURN origin.name as origin, candidate.name as candidate,
        |SUM(ROUND(r2.weight + (COALESCE(r2.activity, 0) * 2))) as boost
        |ORDER BY boost desc limit 10""".stripMargin.fixNewLines)

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph {Nodes: ['  candidate@60', '  origin@7', 'c'], Rels: ['(  origin@7)--[r1:KNOWS:WORKS_AT]--(c)', '(c)--[r2:KNOWS:WORKS_AT]--(  candidate@60)'], Predicates: ['not r1 = r2', 'not (`  origin@7`)-[`  REL143`:KNOWS]-(`  candidate@60`)', 'type(r1) = type(r2)', '`  origin@7`.name IN ["Clark Kent"]']},InterestingOrder(RequiredOrderCandidate(List(Desc(Variable(boost),Map()))),List()),AggregatingQueryProjection(Map(origin -> Property(Variable(  origin@7),PropertyKeyName(name)), candidate -> Property(Variable(  candidate@60),PropertyKeyName(name))),Map(boost -> FunctionInvocation(Namespace(List()),FunctionName(SUM),false,Vector(FunctionInvocation(Namespace(List()),FunctionName(ROUND),false,Vector(Add(Property(Variable(r2),PropertyKeyName(weight)),Multiply(FunctionInvocation(Namespace(List()),FunctionName(COALESCE),false,Vector(Property(Variable(r2),PropertyKeyName(activity)), SignedDecimalIntegerLiteral(0))),SignedDecimalIntegerLiteral(2)))))))),QueryPagination(None,Some(SignedDecimalIntegerLiteral(10))),Selections(Set())),None)"""

    result should equal(expectation)
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS xyz WITH owner, xyz > 0 as collection WHERE (owner)--() RETURN owner") {
    val query = buildPlannerQuery(
      """MATCH (owner)
        |WITH owner, COUNT(*) AS xyz
        |WITH owner, xyz > 0 as collection
        |WHERE (owner)--()
        |RETURN owner""".stripMargin.fixNewLines)

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph {Nodes: ['owner']},InterestingOrder(RequiredOrderCandidate(List()),List()),AggregatingQueryProjection(Map(owner -> Variable(owner)),Map(xyz -> CountStar()),QueryPagination(None,None),Selections(Set())),Some(RegularPlannerQuery(QueryGraph {Arguments: ['owner', 'xyz']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(owner -> Variable(owner), collection -> GreaterThan(Variable(xyz),SignedDecimalIntegerLiteral(0))),QueryPagination(None,None),Selections(Set(Predicate(Set(owner,   REL90,   NODE92),PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Variable(owner)),List(),None,None),RelationshipPattern(Some(Variable(  REL90)),List(),None,None,BOTH,false,None),NodePattern(Some(Variable(  NODE92)),List(),None,None)))))))),Some(RegularPlannerQuery(QueryGraph {Arguments: ['collection', 'owner']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(owner -> Variable(owner)),QueryPagination(None,None),Selections(Set())),None)))))"""
    result should equal(expectation)
  }

  test("UNWIND [1,2,3] AS x RETURN x") {
    val query = buildPlannerQuery("UNWIND [1,2,3] AS x RETURN x")

    query.horizon should equal(UnwindProjection(
      "x",
      listOfInt(1, 2, 3)
    ))

    val tail = query.tail.get

    tail.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x"))))
  }

  test("CALL foo() YIELD all RETURN all") {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "foo"),
      inputSignature = IndexedSeq.empty,
      deprecationInfo = None,
      outputSignature = Some(IndexedSeq(FieldSignature("all", CTInteger))),
      accessMode = ProcedureReadOnlyAccess(Array.empty),
      id = 42
    )
    val query = buildPlannerQuery("CALL foo() YIELD all RETURN all", Some(_ => signature))

    query.horizon match {
      case ProcedureCallProjection(call) =>
        call.signature should equal(signature)
      case _ =>
        fail("Built wrong query horizon")
    }
  }

  test("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y") {
    val query = buildPlannerQuery("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y")

    query.horizon should equal(RegularQueryProjection(
      projections = Map("xes" -> listOfInt(1, 2, 3), "y" -> literalInt(2))
    ))

    val tail = query.tail.get

    tail.horizon should equal(UnwindProjection(
      "x",
      varFor("xes")
    ))

    val tailOfTail = tail.tail.get
    val graph = tailOfTail.queryGraph

    graph.argumentIds should equal(Set("xes", "x", "y"))
  }

  test("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n") {
    val query = buildPlannerQuery("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n")

    query.horizon should equal(UnwindProjection(
      "x", listOfInt(1, 2, 3)
    ))

    val tail = query.tail.get

    tail.queryGraph.patternNodes should equal(Set("n"))
    val set = Set(Predicate(Set("n", "x"), in(nProp, listOf(varFor("x")))))

    tail.queryGraph.selections.predicates should equal(set)
    tail.horizon should equal(RegularQueryProjection(Map("n" -> varFor("n"))))
  }

  test("MATCH (n) UNWIND n.prop as x RETURN x") {
    val query = buildPlannerQuery("MATCH (n) UNWIND n.prop as x RETURN x")

    query.queryGraph.patternNodes should equal(Set("n"))

    query.horizon should equal(UnwindProjection(
      "x", nProp
    ))

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x"))))
  }

  test("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node") {
    val query = buildPlannerQuery("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node")

    query.queryGraph.patternNodes should equal(Set("row"))

    query.horizon should equal(
      AggregatingQueryProjection(
        groupingExpressions = Map.empty,
        aggregationExpressions = Map("rows" -> collect(varFor("row"))),
        queryPagination = QueryPagination.empty
      )
    )

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(UnwindProjection(
      "node",
      varFor("rows")
    ))

    val tailOfTail = tail.tail.get
    tailOfTail.queryGraph.patternNodes should equal(Set.empty)
    tailOfTail.horizon should equal(
      RegularQueryProjection(
        projections = Map("node" -> varFor("node"))
      )
    )
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2") {
    val query = buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph {Nodes: ['a1', 'b1'], Rels: ['(a1)--[r]->-(b1)']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(r -> Variable(r), a1 -> Variable(a1)),QueryPagination(None,Some(SignedDecimalIntegerLiteral(1))),Selections(Set())),Some(RegularPlannerQuery(QueryGraph {Arguments: ['a1', 'r'], Optional Matches: : ['QueryGraph {Nodes: ['a1', 'b2'], Rels: ['(a1)-<-[r]--(b2)'], Arguments: ['a1', 'r']}']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(a1 -> Variable(a1), r -> Variable(r), b2 -> Variable(b2)),QueryPagination(None,None),Selections(Set())),None)))"""

    result should equal(expectation)
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2") {
    val query = buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2")

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph {Nodes: ['a1', 'b1'], Rels: ['(a1)--[r]->-(b1)']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(r -> Variable(r), a1 -> Variable(a1)),QueryPagination(None,Some(SignedDecimalIntegerLiteral(1))),Selections(Set())),Some(RegularPlannerQuery(QueryGraph {Arguments: ['a1', 'r'], Optional Matches: : ['QueryGraph {Nodes: ['a2', 'b2'], Rels: ['(a2)-<-[r]--(b2)'], Arguments: ['a1', 'r'], Predicates: ['a1 = a2']}']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(a1 -> Variable(a1), r -> Variable(r), b2 -> Variable(b2), a2 -> Variable(a2)),QueryPagination(None,None),Selections(Set())),None)))"""

    result should equal(expectation)
  }

  test("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d") {
    val query = buildPlannerQuery("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")

    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.patternRelationships should equal(Set.empty)

    query.horizon should equal(RegularQueryProjection(
      projections = Map("x" -> function("coalesce", varFor("b"), varFor("c")))
    ))

    val optionalMatch1 = query.queryGraph.optionalMatches(0)
    val optionalMatch2 = query.queryGraph.optionalMatches(1)

    optionalMatch1.argumentIds should equal (Set("a"))
    optionalMatch1.patternNodes should equal (Set("a", "b"))


    optionalMatch2.argumentIds should equal (Set("a"))
    optionalMatch2.patternNodes should equal (Set("a", "c"))

    val tail = query.tail.get
    tail.queryGraph.argumentIds should equal(Set("x"))
    tail.queryGraph.patternNodes should equal(Set("x", "d"))

    tail.queryGraph.optionalMatches should be (empty)
  }

  def relType(name: String): RelTypeName = RelTypeName(name)_
}
