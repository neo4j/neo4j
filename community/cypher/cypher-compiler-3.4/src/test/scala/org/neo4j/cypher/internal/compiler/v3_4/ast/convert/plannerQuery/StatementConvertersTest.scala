/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_4.planner.{LogicalPlanningTestSupport, _}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.v3_4.logical.plans.{FieldSignature, ProcedureReadOnlyAccess, ProcedureSignature, QualifiedName}

class StatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val nIdent: Variable = Variable("n")_
  val A: LabelName = LabelName("A")_
  val B: LabelName = LabelName("B")_
  val X: LabelName = LabelName("X")_
  val Y: LabelName = LabelName("Y")_
  val lit42: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
  val lit43: SignedIntegerLiteral = SignedDecimalIntegerLiteral("43")_

  val patternRel = PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength)
  val nProp: Expression = Property( Variable( "n" ) _, PropertyKeyName( "prop" ) _ ) _

  test("RETURN 42") {
    val query = buildPlannerQuery("RETURN 42")
    query.horizon should equal(RegularQueryProjection(Map("42" -> SignedDecimalIntegerLiteral("42")_)))
  }

  test("RETURN 42, 'foo'") {
    val query = buildPlannerQuery("RETURN 42, 'foo'")
    query.horizon should equal(RegularQueryProjection(Map(
      "42" -> SignedDecimalIntegerLiteral("42")_,
      "'foo'" -> StringLiteral("foo")_
    )))
  }

  test("match (n) return n") {
    val query = buildPlannerQuery("match (n) return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH (n) WHERE n:A:B RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:A:B RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(B))_
      ))))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("match (n) where n:X OR n:Y return n") {
    val query = buildPlannerQuery("match (n) where n:X OR n:Y return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(Y))(pos)
      ))_
      ))))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(B))(pos)
      ))_),
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(A))(pos)
      ))_)
    )))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH (n) WHERE id(n) = 42 RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Variable("n")(pos)))(pos),
        ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
      )_
      ))))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH (n) WHERE id(n) IN [42, 43] RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE id(n) IN [42, 43] RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Variable("n")(pos)))(pos),
        ListLiteral(Seq(lit42, lit43))_
      )_
      ))))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n") {
    val query = buildPlannerQuery("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Variable("n")(pos)))(pos),
        ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
      )_
      ))))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("match (p) = (a) return p") {
    val query = buildPlannerQuery("match p = (a) return p")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set[IdName]("a"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression](
      "p" -> PathExpression(NodePathStep(Variable("a")_, NilPathStep))_
    )))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val query = buildPlannerQuery("match p = (a)-[r]->(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(patternRel))
    query.queryGraph.patternNodes should equal(Set[IdName]("a", "b"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b, c") {
    val query = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), OUTGOING, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Variable("r")_, Variable("r2")_)_)_)
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_,
      "b" -> Variable("b")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val query = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(a) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), OUTGOING, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Variable("r")_, Variable("r2")_)_)_)
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Variable("r")_, Variable("r2")_)_)_)
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Variable("r")_, Variable("r2")_)_)_)
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val query = buildPlannerQuery("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("n"), IdName("c")), BOTH, Seq(relType("Type")), SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("n"), IdName("c")))
    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_)
    )))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type|Foo]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c") {
    val query = buildPlannerQuery("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), OUTGOING, Seq(relType("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), OUTGOING, Seq(relType("FRIEND")), VarPatternLength(0, Some(1)))))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "b" -> Variable("b")_,
      "c" -> Variable("c")_
    )))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*3..]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val query = buildPlannerQuery("match (a)-[r:Type*5]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val query = buildPlannerQuery("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, VarPatternLength(1, None))))
    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))

    val identR = Variable("r")(null)
    val identR2 = Variable("r2")(null)
    val inner = AnyIterablePredicate(FilterScope(identR2, Some(Equals(identR, identR2)(null)))(null), identR2)(null)
    val outer = NoneIterablePredicate(FilterScope(identR, Some(inner))(null), identR)(null)
    val predicate = Predicate(Set(IdName("r2"), IdName("r")), outer)

    query.queryGraph.selections should equal(Selections(Set(predicate)))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "r" -> Variable("r")_
    )))
  }

  test("optional match (a) return a") {
    val query = buildPlannerQuery("optional match (a) return a")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_
    )))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set(IdName("a")))
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
      "a" -> Variable("a")_,
      "b" -> Variable("b")_,
      "r" -> Variable("r")_
    )))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())


    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be (empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match (a) optional match (a)-[r]->(b) return a,b,r") {
    val query = buildPlannerQuery("match (a) optional match (a)-[r]->(b) return a,b,r")
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Variable("a")_,
      "b" -> Variable("b")_,
      "r" -> Variable("r")_
    )))
    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set(IdName("a")))
  }

  test("match (a) where (a)-->() return a") {
    // Given
    val query = buildPlannerQuery("match (a) where (a)-->() return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED20"
    val nodeName = "  UNNAMED23"
    val exp = GreaterThan(GetDegree(varFor("a"),None,OUTGOING) _,
                          SignedDecimalIntegerLiteral("0") _) _
    val predicate= Predicate(Set(IdName("a")), exp)
    val selections = Selections(Set(predicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
  }

  test("match (n) return n.prop order by n.prop2 DESC") {
    val query = buildPlannerQuery("match (n) return n.prop order by n.prop2 DESC")
    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph(Set(),Set(IdName(n)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(n -> Variable(n),   FRESHID19 -> Property(Variable(n),PropertyKeyName(prop))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(n), IdName(  FRESHID19)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(  FRESHID19 -> Variable(  FRESHID19),   FRESHID35 -> Property(Variable(n),PropertyKeyName(prop2))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID19), IdName(  FRESHID35)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(  FRESHID19 -> Variable(  FRESHID19),   FRESHID35 -> Variable(  FRESHID35)),QueryShuffle(Vector(DescSortItem(Variable(  FRESHID35))),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID19), IdName(  FRESHID35)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(n.prop -> Variable(  FRESHID19)),QueryShuffle(List(),None,None)),None)))))))"""

    result should equal(expectation)
  }

  test("MATCH (a) WITH 1 as b RETURN b") {
    val query = buildPlannerQuery("MATCH (a) WITH 1 as b RETURN b")
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(Some(RegularPlannerQuery(QueryGraph(Set.empty, Set.empty, Set(IdName("b"))), RegularQueryProjection(Map("b" -> Variable("b") _)))))
  }

  test("WITH 1 as b RETURN b") {
    val query = buildPlannerQuery("WITH 1 as b RETURN b")

    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(Some(RegularPlannerQuery(QueryGraph(Set.empty, Set.empty, Set(IdName("b"))), RegularQueryProjection(Map("b" -> Variable("b") _)))))
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val query = buildPlannerQuery("MATCH (a) WITH a WHERE TRUE RETURN a")
    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph(Set(),Set(IdName(a)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(a -> Variable(a),   FRESHID23 -> True()),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(a), IdName(  FRESHID23)),Selections(Set(Predicate(Set(IdName(  FRESHID23)),Variable(  FRESHID23)))),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(a -> Variable(a)),QueryShuffle(List(),None,None)),None)))"""
    result should equal(expectation)
  }

  test("match (a) where a.prop = 42 OR (a)-->() return a") {
    // Given
    val query = buildPlannerQuery("match (a) where a.prop = 42 OR (a)-->() return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED35"
    val nodeName = "  UNNAMED38"
    val exp1 = GreaterThan(GetDegree(varFor("a"),None,OUTGOING) _, SignedDecimalIntegerLiteral("0") _) _
    val exp2: Expression = In(
      Property(Variable("a")_, PropertyKeyName("prop")_)_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
  }

  test("match (a) where (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildPlannerQuery("match (a) where (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED20"
    val nodeName = "  UNNAMED23"
    val exp1 = GreaterThan(GetDegree(varFor("a"),None,OUTGOING) _, SignedDecimalIntegerLiteral("0") _) _
    val exp2: Expression = In(
      Property(Variable("a")_, PropertyKeyName("prop")_)_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    ) _
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
  }

  test("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildPlannerQuery("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED36"
    val nodeName = "  UNNAMED39"
    val exp1 = GreaterThan(GetDegree(varFor("a"),None,OUTGOING)_, SignedDecimalIntegerLiteral("0")_)_
    val exp2: Expression = In(
      Property(Variable("a")_, PropertyKeyName("prop")_)_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val exp3: Expression = In(
      Property(Variable("a")_, PropertyKeyName("prop2")_)_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("21")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp3, exp2))_)

    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
  }

  test("match (n) return n limit 10") {
    // Given
    val query = buildPlannerQuery("match (n) return n limit 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->varFor("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(SignedDecimalIntegerLiteral("10")(pos)))))
  }

  test("match (n) return n skip 10") {
    // Given
    val query = buildPlannerQuery("match (n) return n skip 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->varFor("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = Some(SignedDecimalIntegerLiteral("10")(pos)),
          limit = None)))
  }

  test("match (a) with * return a") {
    val query = buildPlannerQuery("match (a) with * return a")
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression]("a" -> Variable("a")_)))
    query.tail should equal(None)
  }

  test("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b") {
    val query = buildPlannerQuery("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b")
    query.queryGraph should equal(
      QueryGraph
        .empty
        .addPatternNodes(IdName("a"))
    )

    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("a" -> Variable("a")_),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(SignedDecimalIntegerLiteral("1")(pos)))))

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
            .addPatternNodes(IdName("a"))
            .addSelections(Selections(Set(Predicate(Set("a"), HasLabels(varFor("a"), Seq(LabelName("Foo")_))_))))
        ))
    query.horizon should equal(RegularQueryProjection(Map("a" -> Variable("a")_)))
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
      Predicate(Set(IdName("a")), HasLabels(Variable("a")_, Seq(LabelName("Start")(null)))_)
    ))
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)),
        QueryShuffle(Seq.empty, None, Some(SignedDecimalIntegerLiteral("1")(pos)))
      )
    )
    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set(IdName("b")))
    tailQg.queryGraph.patternRelationships should be(empty)

    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Variable("b") _) _, ListLiteral(Seq(Variable("property")(pos))) _) _
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryShuffle()))
  }

  test("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b")
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Variable("a") _, Seq(LabelName("Start")(null))) _)
    ))

    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> Property(Variable("a")_, PropertyKeyName("prop")_)_)))

    val secondQuery = query.tail.get
    secondQuery.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Variable("b") _) _, ListLiteral(Seq(Variable("property")(pos))) _) _
      )))

    secondQuery.horizon should equal(
      RegularQueryProjection(
        Map("b" -> Variable("b")_)))
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b")
    query.tail should not be empty
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Variable("a")_, Seq(LabelName("Start")(null)))_)
    ))
    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(AggregatingQueryProjection(
      Map("property" -> Property(Variable("a")_, PropertyKeyName("prop")_)_),
      Map("count" -> CountStar()_)
    ))

    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set(IdName("b")))
    tailQg.queryGraph.patternRelationships should be(empty)
    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Variable("b") _) _, ListLiteral(Seq(Variable("property") _)) _) _
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryShuffle()))
  }

  test("MATCH (n) RETURN count(*)") {
    val query = buildPlannerQuery("MATCH (n) RETURN count(*)")

    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregationExpression, QueryShuffle(sorting, limit, skip)) =>
        groupingKeys should be (empty)
        sorting should be (empty)
        limit should be (empty)
        skip should be (empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be (empty)
    query.queryGraph.patternRelationships should be (empty)
    query.queryGraph.patternNodes should be (Set(IdName("n")))
  }

  test("MATCH (n) RETURN n.prop, count(*)") {
    val query = buildPlannerQuery("MATCH (n) RETURN n.prop, count(*)")

    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregationExpression, QueryShuffle(sorting, limit, skip)) =>
        groupingKeys should equal(Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
        sorting should be (empty)
        limit should be (empty)
        skip should be (empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be (empty)
    query.queryGraph.patternRelationships should be (empty)
    query.queryGraph.patternNodes should be (Set(IdName("n")))
  }

  test("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n") {
    val query = buildPlannerQuery("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n")

    query.queryGraph.hints should equal(Set[Hint](UsingIndexHint(varFor("n"), LabelName("Awesome")_, Seq(PropertyKeyName("prop")(pos)))_))
  }

  test("MATCH shortestPath((a)-[r]->(b)) RETURN r") {
    val query = buildPlannerQuery("MATCH shortestPath((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some("  FRESHID6"), PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH allShortestPaths((a)-[r]->(b)) RETURN r") {
    val query = buildPlannerQuery("MATCH allShortestPaths((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some("  FRESHID6"), PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = false)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH p = shortestPath((a)-[r]->(b)) RETURN p") {
    val query = buildPlannerQuery("MATCH p = shortestPath((a)-[r]->(b)) RETURN p")

    query.queryGraph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.queryGraph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some(IdName("p")), PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("match (n) return distinct n") {
    val query = buildPlannerQuery("match (n) return distinct n")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("match (n) with distinct n.prop as x return x") {
    val query = buildPlannerQuery("match (n) with distinct n.prop as x return x")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("x" -> nProp)
    ))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("match (n) with distinct * return n") {
    val query = buildPlannerQuery("match (n) with distinct * return n")
    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set(IdName("n")))
  }

  test("WITH DISTINCT 1 as b RETURN b") {
    val query = buildPlannerQuery("WITH DISTINCT 1 as b RETURN b")

    query.horizon should equal(DistinctQueryProjection(
      groupingKeys = Map("b" -> SignedDecimalIntegerLiteral("1") _)
    ))

    query.tail should not be empty
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner") {
    val query = buildPlannerQuery("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner")
    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph(Set(),Set(IdName(owner)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),AggregatingQueryProjection(Map(owner -> Variable(owner)),Map(collected -> CountStar()),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collected)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(owner -> Variable(owner), collected -> Variable(collected),   FRESHID54 -> PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Variable(owner)),List(),None),RelationshipPattern(Some(Variable(  UNNAMED62)),List(),None,None,BOTH,false),NodePattern(Some(Variable(  UNNAMED64)),List(),None))))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collected), IdName(  FRESHID54)),Selections(Set(Predicate(Set(IdName(  FRESHID54)),Variable(  FRESHID54)))),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(owner -> Variable(owner)),QueryShuffle(List(),None,None)),None)))))"""

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
      """RegularPlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r1),(IdName(  origin@7),IdName(c)),BOTH,List(RelTypeName(KNOWS), RelTypeName(WORKS_AT)),SimplePatternLength), PatternRelationship(IdName(r2),(IdName(c),IdName(  candidate@60)),BOTH,List(RelTypeName(KNOWS), RelTypeName(WORKS_AT)),SimplePatternLength)),Set(IdName(  origin@7), IdName(c), IdName(  candidate@60)),Set(),Selections(Set(Predicate(Set(IdName(r1), IdName(r2)),Not(Equals(Variable(r1),Variable(r2)))), Predicate(Set(IdName(  origin@7), IdName(  candidate@60)),Not(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Variable(  origin@7)),List(),None),RelationshipPattern(Some(Variable(  UNNAMED143)),List(RelTypeName(KNOWS)),None,None,BOTH,false),NodePattern(Some(Variable(  candidate@60)),List(),None)))))), Predicate(Set(IdName(r1), IdName(r2)),Equals(FunctionInvocation(Namespace(List()),FunctionName(type),false,Vector(Variable(r1))),FunctionInvocation(Namespace(List()),FunctionName(type),false,Vector(Variable(r2))))), Predicate(Set(IdName(  origin@7)),In(Property(Variable(  origin@7),PropertyKeyName(name)),ListLiteral(List(StringLiteral(Clark Kent))))))),Vector(),Set(),Set(),List()),AggregatingQueryProjection(Map(  FRESHID178 -> Property(Variable(  origin@7),PropertyKeyName(name)),   FRESHID204 -> Property(Variable(  candidate@60),PropertyKeyName(name))),Map(  FRESHID223 -> FunctionInvocation(Namespace(List()),FunctionName(SUM),false,Vector(FunctionInvocation(Namespace(List()),FunctionName(ROUND),false,Vector(Add(Property(Variable(r2),PropertyKeyName(weight)),Multiply(FunctionInvocation(Namespace(List()),FunctionName(COALESCE),false,Vector(Property(Variable(r2),PropertyKeyName(activity)), SignedDecimalIntegerLiteral(0))),SignedDecimalIntegerLiteral(2)))))))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID178), IdName(  FRESHID204), IdName(  FRESHID223)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(  FRESHID178 -> Variable(  FRESHID178),   FRESHID204 -> Variable(  FRESHID204),   FRESHID223 -> Variable(  FRESHID223)),QueryShuffle(List(DescSortItem(Variable(  FRESHID223))),None,Some(SignedDecimalIntegerLiteral(10)))),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID178), IdName(  FRESHID204), IdName(  FRESHID223)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(origin -> Variable(  FRESHID178), candidate -> Variable(  FRESHID204), boost -> Variable(  FRESHID223)),QueryShuffle(List(),None,None)),None)))))"""

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
      """RegularPlannerQuery(QueryGraph(Set(),Set(IdName(owner)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),AggregatingQueryProjection(Map(owner -> Variable(owner)),Map(xyz -> CountStar()),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(xyz)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(owner -> Variable(owner), collection -> GreaterThan(Variable(xyz),SignedDecimalIntegerLiteral(0))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collection)),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(owner -> Variable(owner), collection -> Variable(collection),   FRESHID82 -> PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Variable(owner)),List(),None),RelationshipPattern(Some(Variable(  UNNAMED90)),List(),None,None,BOTH,false),NodePattern(Some(Variable(  UNNAMED92)),List(),None))))),QueryShuffle(List(),None,None)),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collection), IdName(  FRESHID82)),Selections(Set(Predicate(Set(IdName(  FRESHID82)),Variable(  FRESHID82)))),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(owner -> Variable(owner)),QueryShuffle(List(),None,None)),None)))))))""".stripMargin

    result should equal(expectation)
  }

  test("UNWIND [1,2,3] AS x RETURN x") {
    val query = buildPlannerQuery("UNWIND [1,2,3] AS x RETURN x")

    val one = SignedDecimalIntegerLiteral("1")(pos)
    val two = SignedDecimalIntegerLiteral("2")(pos)
    val three = SignedDecimalIntegerLiteral("3")(pos)

    query.horizon should equal(UnwindProjection(
      IdName("x"),
      ListLiteral(Seq(one, two, three))(pos)
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
      accessMode = ProcedureReadOnlyAccess(Array.empty)
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

    val one = SignedDecimalIntegerLiteral("1")(pos)
    val two = SignedDecimalIntegerLiteral("2")(pos)
    val three = SignedDecimalIntegerLiteral("3")(pos)

    query.horizon should equal(RegularQueryProjection(
      projections = Map("xes" -> ListLiteral(Seq(one, two, three))(pos), "y" -> two)
    ))

    val tail = query.tail.get

    tail.horizon should equal(UnwindProjection(
      IdName("x"),
      varFor("xes")
    ))

    val tailOfTail = tail.tail.get
    val graph = tailOfTail.queryGraph

    graph.argumentIds should equal(Set(IdName("xes"), IdName("x"), IdName("y")))
  }

  private val one = SignedDecimalIntegerLiteral("1")(pos)
  private val two = SignedDecimalIntegerLiteral("2")(pos)
  private val three = SignedDecimalIntegerLiteral("3")(pos)
  private val collection = ListLiteral(Seq(one, two, three))(pos)

  test("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n") {
    val query = buildPlannerQuery("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n")

    query.horizon should equal(UnwindProjection(
      IdName("x"), collection
    ))

    val tail = query.tail.get

    tail.queryGraph.patternNodes should equal(Set(IdName("n")))
    val set: Set[Predicate] = Set(
      Predicate(Set(IdName("n"), IdName("x")), In(nProp, ListLiteral(Seq(varFor("x"))) _) _))

    tail.queryGraph.selections.predicates should equal(set)
    tail.horizon should equal(RegularQueryProjection(Map("n" -> varFor("n"))))
  }

  test("MATCH (n) UNWIND n.prop as x RETURN x") {
    val query = buildPlannerQuery("MATCH (n) UNWIND n.prop as x RETURN x")

    query.queryGraph.patternNodes should equal(Set(IdName("n")))

    query.horizon should equal(UnwindProjection(
      IdName("x"),
      nProp
    ))

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x"))))
  }

  test("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node") {
    val query = buildPlannerQuery("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node")

    query.queryGraph.patternNodes should equal(Set(IdName("row")))

    val functionName: FunctionName = FunctionName("collect") _
    val functionInvocation: FunctionInvocation = FunctionInvocation(functionName, varFor("row")) _

    query.horizon should equal(
      AggregatingQueryProjection(
        groupingExpressions = Map.empty,
        aggregationExpressions = Map("rows" -> functionInvocation),
        shuffle = QueryShuffle.empty
      )
    )

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(UnwindProjection(
      IdName("node"),
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

  test("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b") {
    val query = buildPlannerQuery("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2") {
    val query = buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b1)),OUTGOING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b1)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(r -> Variable(r), a1 -> Variable(a1)),QueryShuffle(List(),None,Some(SignedDecimalIntegerLiteral(1)))),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(r), IdName(a1)),Selections(Set()),Vector(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b2)),INCOMING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b2)),Set(IdName(r), IdName(a1)),Selections(Set()),Vector(),Set(),Set(),List())),Set(),Set(),List()),RegularQueryProjection(Map(a1 -> Variable(a1), r -> Variable(r), b2 -> Variable(b2)),QueryShuffle(List(),None,None)),None)))"""

    result should equal(expectation)
  }

  // scalastyle:off
  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2") {
    val query = buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2")

    val result = query.toString

    val expectation =
      """RegularPlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b1)),OUTGOING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b1)),Set(),Selections(Set()),Vector(),Set(),Set(),List()),RegularQueryProjection(Map(r -> Variable(r), a1 -> Variable(a1)),QueryShuffle(List(),None,Some(SignedDecimalIntegerLiteral(1)))),Some(RegularPlannerQuery(QueryGraph(Set(),Set(),Set(IdName(r), IdName(a1)),Selections(Set()),Vector(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a2),IdName(b2)),INCOMING,List(),SimplePatternLength)),Set(IdName(a2), IdName(b2)),Set(IdName(r), IdName(a1)),Selections(Set(Predicate(Set(IdName(a1), IdName(a2)),Equals(Variable(a1),Variable(a2))))),Vector(),Set(),Set(),List())),Set(),Set(),List()),RegularQueryProjection(Map(a1 -> Variable(a1), r -> Variable(r), b2 -> Variable(b2), a2 -> Variable(a2)),QueryShuffle(List(),None,None)),None)))"""

    result should equal(expectation)
  }
  // scalastyle:on

  test("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d") {
    val query = buildPlannerQuery("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")

    query.queryGraph.patternNodes should equal(Set(IdName("a")))
    query.queryGraph.patternRelationships should equal(Set.empty)

    query.horizon should equal(RegularQueryProjection(
      projections = Map(
        "x" -> FunctionInvocation(FunctionName("coalesce")_, distinct = false, Vector(varFor("b"), varFor("c")))(pos)
      )
    ))

    val optionalMatch1 = query.queryGraph.optionalMatches(0)
    val optionalMatch2 = query.queryGraph.optionalMatches(1)

    optionalMatch1.argumentIds should equal (Set(IdName("a")))
    optionalMatch1.patternNodes should equal (Set(IdName("a"), IdName("b")))


    optionalMatch2.argumentIds should equal (Set(IdName("a")))
    optionalMatch2.patternNodes should equal (Set(IdName("a"), IdName("c")))

    val tail = query.tail.get
    tail.queryGraph.argumentIds should equal(Set(IdName("x")))
    tail.queryGraph.patternNodes should equal(Set(IdName("x"), IdName("d")))

    tail.queryGraph.optionalMatches should be (empty)
  }

  def relType(name: String): RelTypeName = RelTypeName(name)_
}
