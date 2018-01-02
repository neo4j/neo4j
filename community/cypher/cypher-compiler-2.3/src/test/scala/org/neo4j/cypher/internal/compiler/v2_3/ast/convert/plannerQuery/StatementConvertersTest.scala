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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters.{namePatternPredicatePatternElements, normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, _}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, inSequence}

class StatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val nIdent: Identifier = Identifier("n")_
  val A: LabelName = LabelName("A")_
  val B: LabelName = LabelName("B")_
  val X: LabelName = LabelName("X")_
  val Y: LabelName = LabelName("Y")_
  val lit42: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
  val lit43: SignedIntegerLiteral = SignedDecimalIntegerLiteral("43")_

  val patternRel = PatternRelationship("r", ("a", "b"), OUTGOING, Seq.empty, SimplePatternLength)
  val nProp: Expression = Property( Identifier( "n" ) _, PropertyKeyName( "prop" ) _ ) _

  def buildPlannerQuery(query: String, cleanStatement: Boolean = true): UnionQuery = {
    val ast = parser.parse(query.replace("\r\n", "\n"))
    val mkException = new SyntaxExceptionCreator(query, Some(pos))
    val cleanedStatement: Statement =
      if (cleanStatement)
        ast.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
      else
        ast

    val semanticChecker = new SemanticChecker
    val semanticState = semanticChecker.check(query, cleanedStatement, mkException)
    val statement = astRewriter.rewrite(query, cleanedStatement, semanticState)._1
    val semanticTable: SemanticTable = SemanticTable(types = semanticState.typeTable)
    val (rewrittenAst: Statement, _) = CostBasedExecutablePlanBuilder.rewriteStatement(statement, semanticState.scopeTree, semanticTable, RewriterStepSequencer.newValidating, semanticChecker, Set.empty, mock[AstRewritingMonitor])

    // This fakes pattern expression naming for testing purposes
    // In the actual code path, this renaming happens as part of planning
    //
    // cf. QueryPlanningStrategy
    //

    val namedAst = rewrittenAst.endoRewrite(namePatternPredicatePatternElements)
    val unionQuery = namedAst.asInstanceOf[Query].asUnionQuery
    unionQuery
  }

  test("RETURN 42") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("RETURN 42")
    query.horizon should equal(RegularQueryProjection(Map("42" -> SignedDecimalIntegerLiteral("42")_)))
  }

  test("RETURN 42, 'foo'") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("RETURN 42, 'foo'")
    query.horizon should equal(RegularQueryProjection(Map(
      "42" -> SignedDecimalIntegerLiteral("42")_,
      "'foo'" -> StringLiteral("foo")_
    )))
  }

  test("match n return n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:A:B RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n WHERE n:A:B RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(B))_
      ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match n where n:X OR n:Y return n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n where n:X OR n:Y return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(Y))(pos)
      ))_
      ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(B))(pos)
      ))_),
      Predicate(Set(IdName("n")), Ors(Set(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(A))(pos)
      ))_)
    )))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) = 42 RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n WHERE id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
      )_
      ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) IN [42, 43] RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n WHERE id(n) IN [42, 43] RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(lit42, lit43))_
      )_
      ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:A AND id(n) = 42 RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n WHERE n:A AND id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
      )_
      ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match p = (a) return p") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match p = (a) return p")
    query.graph.patternRelationships should equal(Set())
    query.graph.patternNodes should equal(Set[IdName]("a"))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression](
      "p" -> PathExpression(NodePathStep(Identifier("a")_, NilPathStep))_
    )))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match p = (a)-[r]->(b) return a,r")
    query.graph.patternRelationships should equal(Set(patternRel))
    query.graph.patternNodes should equal(Set[IdName]("a", "b"))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b, c") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), OUTGOING, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Identifier("r")_, Identifier("r2")_)_)_)
    query.graph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_,
      "b" -> Identifier("b")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(a) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), OUTGOING, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Identifier("r")_, Identifier("r2")_)_)_)
    query.graph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Identifier("r")_, Identifier("r2")_)_)_)
    query.graph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    val predicate: Predicate = Predicate(Set(IdName("r"), IdName("r2")), Not(Equals(Identifier("r")_, Identifier("r2")_)_)_)
    query.graph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("n"), IdName("c")), BOTH, Seq(relType("Type")), SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("n"), IdName("c")))
    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_)
    )))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r:Type|Foo]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r:Type*]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), OUTGOING, Seq(relType("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), OUTGOING, Seq(relType("FRIEND")), VarPatternLength(0, Some(1)))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "c" -> Identifier("c")_
    )))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r:Type*3..]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)-[r:Type*5]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), BOTH, Seq.empty, VarPatternLength(1, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))

    val identR = Identifier("r")(null)
    val identR2 = Identifier("r2")(null)
    val inner = AnyIterablePredicate(FilterScope(identR2, Some(Equals(identR, identR2)(null)))(null), identR2)(null)
    val outer = NoneIterablePredicate(FilterScope(identR, Some(inner))(null), identR)(null)
    val predicate = Predicate(Set(IdName("r2"), IdName("r")), outer)

    query.graph.selections should equal(Selections(Set(predicate)))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("optional match (a) return a") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("optional match (a) return a")
    query.graph.patternRelationships should equal(Set())
    query.graph.patternNodes should equal(Set())
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_
    )))

    query.graph.optionalMatches.size should equal(1)
    query.graph.argumentIds should equal(Set())

    val optMatchQG = query.graph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set(IdName("a")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("optional match (a)-[r]->(b) return a,b,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("optional match (a)-[r]->(b) return a,b,r")
    query.graph.patternRelationships should equal(Set())
    query.graph.patternNodes should equal(Set())
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    )))

    query.graph.optionalMatches.size should equal(1)
    query.graph.argumentIds should equal(Set())


    val optMatchQG = query.graph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be (empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match a optional match (a)-[r]->(b) return a,b,r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match a optional match (a)-[r]->(b) return a,b,r")
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.graph.patternRelationships should equal(Set())
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    )))
    query.graph.optionalMatches.size should equal(1)
    query.graph.argumentIds should equal(Set())

    val optMatchQG = query.graph.optionalMatches.head
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set(IdName("a")))
  }

  test("match a where (a)-->() return a") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match a where (a)-->() return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED18"
    val nodeName = "  UNNAMED21"
    val exp: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val predicate= Predicate(Set(IdName("a")), exp)
    val selections = Selections(Set(predicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
  }

  test("match n return n.prop order by n.prop2 DESC") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n return n.prop order by n.prop2 DESC")
    val result = query.toString

    val expectation =
      """PlannerQuery(QueryGraph(Set(),Set(IdName(n)),Set(),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(n -> Identifier(n),   FRESHID17 -> Property(Identifier(n),PropertyKeyName(prop))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(n), IdName(  FRESHID17)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(  FRESHID17 -> Identifier(  FRESHID17),   FRESHID33 -> Property(Identifier(n),PropertyKeyName(prop2))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID17), IdName(  FRESHID33)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(  FRESHID17 -> Identifier(  FRESHID17),   FRESHID33 -> Identifier(  FRESHID33)),QueryShuffle(Vector(DescSortItem(Identifier(  FRESHID33))),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID17), IdName(  FRESHID33)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(n.prop -> Identifier(  FRESHID17)),QueryShuffle(List(),None,None)),None)))))))""".stripMargin

    result should equal(expectation)
  }

  test("MATCH (a) WITH 1 as b RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (a) WITH 1 as b RETURN b")
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(Some(PlannerQuery(QueryGraph(Set.empty, Set.empty, Set(IdName("b"))), RegularQueryProjection(Map("b" -> Identifier("b") _)))))
  }

  test("WITH 1 as b RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("WITH 1 as b RETURN b")

    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(Some(PlannerQuery(QueryGraph(Set.empty, Set.empty, Set(IdName("b"))), RegularQueryProjection(Map("b" -> Identifier("b") _)))))
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (a) WITH a WHERE TRUE RETURN a")
    val result = query.toString


    val expectation =
      """PlannerQuery(QueryGraph(Set(),Set(IdName(a)),Set(),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(a -> Identifier(a),   FRESHID23 -> True()),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(a), IdName(  FRESHID23)),Selections(Set(Predicate(Set(IdName(  FRESHID23)),Identifier(  FRESHID23)))),List(),Set(),Set()),RegularQueryProjection(Map(a -> Identifier(a)),QueryShuffle(List(),None,None)),None)))""".stripMargin
    result should equal(expectation)
  }

  test("match a where a.prop = 42 OR (a)-->() return a") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match a where a.prop = 42 OR (a)-->() return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED33"
    val nodeName = "  UNNAMED36"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
  }

  test("match a where (a)-->() OR a.prop = 42 return a") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match a where (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED18"
    val nodeName = "  UNNAMED21"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    ) _
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
  }

  test("match a where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match a where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "  UNNAMED34"
    val nodeName = "  UNNAMED37"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val exp3: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop2")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("21")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp3, exp2))_)

    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
  }

  test("match n return n limit 10") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n return n limit 10")

    // Then inner pattern query graph
    query.graph.selections should equal(Selections())
    query.graph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->ident("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(SignedDecimalIntegerLiteral("10")(pos)))))
  }

  test("match n return n skip 10") {
    // Given
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n return n skip 10")

    // Then inner pattern query graph
    query.graph.selections should equal(Selections())
    query.graph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->ident("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = Some(SignedDecimalIntegerLiteral("10")(pos)),
          limit = None)))
  }

  test("match (a) with * return a") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match (a) with * return a")
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression]("a" -> Identifier("a")_)))
    query.tail should equal(None)
  }

  test("MATCH a WITH a LIMIT 1 MATCH a-[r]->b RETURN a, b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH a WITH a LIMIT 1 MATCH a-[r]->b RETURN a, b")
    query.graph should equal(
      QueryGraph
        .empty
        .addPatternNodes(IdName("a"))
    )

    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("a" -> Identifier("a")_),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(SignedDecimalIntegerLiteral("1")(pos)))))

    query.tail should not be empty
    query.tail.get.graph should equal(
      QueryGraph
        .empty
        .addArgumentIds(Seq("a"))
        .addPatternNodes("a", "b")
        .addPatternRelationship(patternRel)
    )
  }

  test("optional match (a:Foo) with a match (a)-[r]->(b) return a") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("optional match (a:Foo) with a match (a)-[r]->(b) return a")

    query.graph should equal(
      QueryGraph
        .empty
        .withAddedOptionalMatch(
          QueryGraph
            .empty
            .addPatternNodes(IdName("a"))
            .addSelections(Selections(Set(Predicate(Set("a"), HasLabels(ident("a"), Seq(LabelName("Foo")_))_))))
        ))
    query.horizon should equal(RegularQueryProjection(Map("a" -> Identifier("a")_)))
    query.tail should not be empty
    query.tail.get.graph should equal(
      QueryGraph
        .empty
        .addArgumentIds(Seq("a"))
        .addPatternNodes("a", "b")
        .addPatternRelationship(patternRel)
    )
  }

  test("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b")
    query.tail should not be empty
    query.graph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Identifier("a")_, Seq(LabelName("Start")(null)))_)
    ))
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> Property(Identifier("a")(pos), PropertyKeyName("prop")(pos))(pos)),
        QueryShuffle(Seq.empty, None, Some(SignedDecimalIntegerLiteral("1")(pos)))
      )
    )
    val tailQg = query.tail.get
    tailQg.graph.patternNodes should equal(Set(IdName("b")))
    tailQg.graph.patternRelationships should be(empty)

    tailQg.graph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Identifier("b") _) _, Collection(Seq(Identifier("property")(pos))) _) _
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> ident("b")),
        QueryShuffle()))
  }

  test("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b")
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.graph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Identifier("a") _, Seq(LabelName("Start")(null))) _)
    ))

    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> Property(Identifier("a")_, PropertyKeyName("prop")_)_)))

    val secondQuery = query.tail.get
    secondQuery.graph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Identifier("b") _) _, Collection(Seq(Identifier("property")(pos))) _) _
      )))

    secondQuery.horizon should equal(
      RegularQueryProjection(
        Map("b" -> Identifier("b")_)))
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b")
    query.tail should not be empty
    query.graph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Identifier("a")_, Seq(LabelName("Start")(null)))_)
    ))
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(AggregatingQueryProjection(
      Map("property" -> Property(Identifier("a")_, PropertyKeyName("prop")_)_),
      Map("count" -> CountStar()_)
    ))

    val tailQg = query.tail.get
    tailQg.graph.patternNodes should equal(Set(IdName("b")))
    tailQg.graph.patternRelationships should be(empty)
    tailQg.graph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("property")),
        In(FunctionInvocation(FunctionName("id") _, Identifier("b") _) _, Collection(Seq(Identifier("property") _)) _) _
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> ident("b")),
        QueryShuffle()))
  }

  test("MATCH n RETURN count(*)") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n RETURN count(*)")

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

    query.graph.selections.predicates should be (empty)
    query.graph.patternRelationships should be (empty)
    query.graph.patternNodes should be (Set(IdName("n")))
  }

  test("MATCH n RETURN n.prop, count(*)") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH n RETURN n.prop, count(*)")

    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregationExpression, QueryShuffle(sorting, limit, skip)) =>
        groupingKeys should equal(Map("n.prop" -> Property(Identifier("n")(pos), PropertyKeyName("prop")(pos))(pos)))
        sorting should be (empty)
        limit should be (empty)
        skip should be (empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.graph.selections.predicates should be (empty)
    query.graph.patternRelationships should be (empty)
    query.graph.patternNodes should be (Set(IdName("n")))
  }

  test("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n")

    query.graph.hints should equal(Set[Hint](UsingIndexHint(ident("n"), LabelName("Awesome")_, PropertyKeyName("prop")(pos))_))
  }

  test("MATCH shortestPath(a-[r]->b) RETURN r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH shortestPath(a-[r]->b) RETURN r")

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(None, PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH allShortestPaths(a-[r]->b) RETURN r") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH allShortestPaths(a-[r]->b) RETURN r")

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(None, PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = false)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH p = shortestPath(a-[r]->b) RETURN p") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH p = shortestPath(a-[r]->b) RETURN p")

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some(IdName("p")), PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("RETURN 1 as x UNION RETURN 2 as x") {
    val query = buildPlannerQuery("RETURN 1 as x UNION RETURN 2 as x")
    query.distinct should equal(true)
    query.queries should have size 2

    val q1 = query.queries.head
    q1.graph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("1")(pos))))

    val q2 = query.queries.last
    q2.graph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("2")(pos))))
  }

  test("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x") {
    val query = buildPlannerQuery("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x")
    query.distinct should equal(false)
    query.queries should have size 3

    val q1 = query.queries.head
    q1.graph.patternNodes shouldBe empty
    q1.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("1")(pos))))

    val q2 = query.queries.tail.head
    q2.graph.patternNodes shouldBe empty
    q2.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("2")(pos))))

    val q3 = query.queries.last
    q3.graph.patternNodes shouldBe empty
    q3.horizon should equal(RegularQueryProjection(Map("x" -> SignedDecimalIntegerLiteral("3")(pos))))
  }

  test("match n return distinct n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n return distinct n")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("n" -> ident("n")),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match n with distinct n.prop as x return x") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n with distinct n.prop as x return x")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("x" -> nProp),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match n with distinct * return n") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("match n with distinct * return n")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("n" -> ident("n")),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("WITH DISTINCT 1 as b RETURN b") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("WITH DISTINCT 1 as b RETURN b")

    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("b" -> SignedDecimalIntegerLiteral("1") _),
      aggregationExpressions = Map.empty
    ))

    query.tail should not be empty
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner")
    val result = query.toString

    val expectation =
      """PlannerQuery(QueryGraph(Set(),Set(IdName(owner)),Set(),Selections(Set()),List(),Set(),Set()),AggregatingQueryProjection(Map(owner -> Identifier(owner)),Map(collected -> CountStar()),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collected)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(owner -> Identifier(owner), collected -> Identifier(collected),   FRESHID54 -> PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Identifier(owner)),List(),None,false),RelationshipPattern(Some(Identifier(  UNNAMED62)),false,List(),None,None,BOTH),NodePattern(Some(Identifier(  UNNAMED64)),List(),None,false))))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collected), IdName(  FRESHID54)),Selections(Set(Predicate(Set(IdName(  FRESHID54)),Identifier(  FRESHID54)))),List(),Set(),Set()),RegularQueryProjection(Map(owner -> Identifier(owner)),QueryShuffle(List(),None,None)),None)))))""".stripMargin

    result should equal(expectation)
  }

  test("Funny query from boostingRecommendations") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery(
      """MATCH (origin)-[r1:KNOWS|WORKS_AT]-(c)-[r2:KNOWS|WORKS_AT]-(candidate)
        |WHERE origin.name = "Clark Kent"
        |AND type(r1)=type(r2) AND NOT (origin)-[:KNOWS]-(candidate)
        |RETURN origin.name as origin, candidate.name as candidate,
        |SUM(ROUND(r2.weight + (COALESCE(r2.activity, 0) * 2))) as boost
        |ORDER BY boost desc limit 10""".stripMargin)

    val result = query.toString

    val expectation =
      """PlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r1),(IdName(  origin@7),IdName(c)),BOTH,List(RelTypeName(KNOWS), RelTypeName(WORKS_AT)),SimplePatternLength), PatternRelationship(IdName(r2),(IdName(c),IdName(  candidate@60)),BOTH,List(RelTypeName(KNOWS), RelTypeName(WORKS_AT)),SimplePatternLength)),Set(IdName(  origin@7), IdName(c), IdName(  candidate@60)),Set(),Selections(Set(Predicate(Set(IdName(r1), IdName(r2)),Not(Equals(Identifier(r1),Identifier(r2)))), Predicate(Set(IdName(  origin@7), IdName(  candidate@60)),Not(PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Identifier(  origin@7)),List(),None,false),RelationshipPattern(Some(Identifier(  UNNAMED143)),false,List(RelTypeName(KNOWS)),None,None,BOTH),NodePattern(Some(Identifier(  candidate@60)),List(),None,false)))))), Predicate(Set(IdName(r1), IdName(r2)),Equals(FunctionInvocation(FunctionName(type),false,Vector(Identifier(r1))),FunctionInvocation(FunctionName(type),false,Vector(Identifier(r2))))), Predicate(Set(IdName(  origin@7)),In(Property(Identifier(  origin@7),PropertyKeyName(name)),Collection(List(StringLiteral(Clark Kent))))))),List(),Set(),Set()),AggregatingQueryProjection(Map(  FRESHID178 -> Property(Identifier(  origin@7),PropertyKeyName(name)),   FRESHID204 -> Property(Identifier(  candidate@60),PropertyKeyName(name))),Map(  FRESHID223 -> FunctionInvocation(FunctionName(SUM),false,Vector(FunctionInvocation(FunctionName(ROUND),false,Vector(Add(Property(Identifier(r2),PropertyKeyName(weight)),Multiply(FunctionInvocation(FunctionName(COALESCE),false,Vector(Property(Identifier(r2),PropertyKeyName(activity)), SignedDecimalIntegerLiteral(0))),SignedDecimalIntegerLiteral(2)))))))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID178), IdName(  FRESHID204), IdName(  FRESHID223)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(  FRESHID178 -> Identifier(  FRESHID178),   FRESHID204 -> Identifier(  FRESHID204),   FRESHID223 -> Identifier(  FRESHID223)),QueryShuffle(List(DescSortItem(Identifier(  FRESHID223))),None,Some(SignedDecimalIntegerLiteral(10)))),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(  FRESHID178), IdName(  FRESHID204), IdName(  FRESHID223)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(origin -> Identifier(  FRESHID178), candidate -> Identifier(  FRESHID204), boost -> Identifier(  FRESHID223)),QueryShuffle(List(),None,None)),None)))))""".stripMargin

    result should equal(expectation)
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS xyz WITH owner, xyz > 0 as collection WHERE (owner)--() RETURN owner") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery(
      """MATCH (owner)
        |WITH owner, COUNT(*) AS xyz
        |WITH owner, xyz > 0 as collection
        |WHERE (owner)--()
        |RETURN owner""".stripMargin)

    val result = query.toString
    val expectation =
      """PlannerQuery(QueryGraph(Set(),Set(IdName(owner)),Set(),Selections(Set()),List(),Set(),Set()),AggregatingQueryProjection(Map(owner -> Identifier(owner)),Map(xyz -> CountStar()),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(xyz)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(owner -> Identifier(owner), collection -> GreaterThan(Identifier(xyz),SignedDecimalIntegerLiteral(0))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collection)),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(owner -> Identifier(owner), collection -> Identifier(collection),   FRESHID82 -> PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(Identifier(owner)),List(),None,false),RelationshipPattern(Some(Identifier(  UNNAMED90)),false,List(),None,None,BOTH),NodePattern(Some(Identifier(  UNNAMED92)),List(),None,false))))),QueryShuffle(List(),None,None)),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(owner), IdName(collection), IdName(  FRESHID82)),Selections(Set(Predicate(Set(IdName(  FRESHID82)),Identifier(  FRESHID82)))),List(),Set(),Set()),RegularQueryProjection(Map(owner -> Identifier(owner)),QueryShuffle(List(),None,None)),None)))))))""".stripMargin

    result should equal(expectation)
  }

  test("UNWIND [1,2,3] AS x RETURN x") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("UNWIND [1,2,3] AS x RETURN x")

    val one = SignedDecimalIntegerLiteral("1")(pos)
    val two = SignedDecimalIntegerLiteral("2")(pos)
    val three = SignedDecimalIntegerLiteral("3")(pos)

    query.horizon should equal(UnwindProjection(
      IdName("x"),
      Collection(Seq(one, two, three))(pos)
    ))

    val tail = query.tail.get

    tail.horizon should equal(RegularQueryProjection(Map("x" -> ident("x"))))
  }

  test("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y") {
    val UnionQuery(query :: Nil, _, _) = buildPlannerQuery("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y")

    val one = SignedDecimalIntegerLiteral("1")(pos)
    val two = SignedDecimalIntegerLiteral("2")(pos)
    val three = SignedDecimalIntegerLiteral("3")(pos)

    query.horizon should equal(RegularQueryProjection(
      projections = Map("xes" -> Collection(Seq(one, two, three))(pos), "y" -> two)
    ))

    val tail = query.tail.get

    tail.horizon should equal(UnwindProjection(
      IdName("x"),
      ident("xes")
    ))

    val tailOfTail = tail.tail.get
    val graph = tailOfTail.graph

    graph.argumentIds should equal(Set(IdName("xes"), IdName("x"), IdName("y")))
  }

  val one = SignedDecimalIntegerLiteral("1")(pos)
  val two = SignedDecimalIntegerLiteral("2")(pos)
  val three = SignedDecimalIntegerLiteral("3")(pos)
  val collection = Collection(Seq(one, two, three))(pos)

  test("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n")

    query.horizon should equal(UnwindProjection(
      IdName("x"), collection
    ))

    val tail = query.tail.get

    tail.graph.patternNodes should equal(Set(IdName("n")))
    val set: Set[Predicate] = Set(
      Predicate(Set(IdName("n"), IdName("x")), In(nProp, Collection(Seq(ident("x"))) _) _))

    tail.graph.selections.predicates should equal(set)
    tail.horizon should equal(RegularQueryProjection(Map("n" -> ident("n"))))
  }

  test("MATCH n UNWIND n.prop as x RETURN x") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH n UNWIND n.prop as x RETURN x")

    query.graph.patternNodes should equal(Set(IdName("n")))

    query.horizon should equal(UnwindProjection(
      IdName("x"),
      nProp
    ))

    val tail = query.tail.get
    tail.graph.patternNodes should equal(Set.empty)
    tail.horizon should equal(RegularQueryProjection(Map("x" -> ident("x"))))
  }

  test("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node")

    query.graph.patternNodes should equal(Set(IdName("row")))

    val functionName: FunctionName = FunctionName("collect") _
    val functionInvocation: FunctionInvocation = FunctionInvocation(functionName, ident("row")) _

    query.horizon should equal(
      AggregatingQueryProjection(
        groupingKeys = Map.empty,
        aggregationExpressions = Map("rows" -> functionInvocation),
        shuffle = QueryShuffle.empty
      )
    )

    val tail = query.tail.get
    tail.graph.patternNodes should equal(Set.empty)
    tail.horizon should equal(UnwindProjection(
      IdName("node"),
      ident("rows")
    ))

    val tailOfTail = tail.tail.get
    tailOfTail.graph.patternNodes should equal(Set.empty)
    tailOfTail.horizon should equal(
      RegularQueryProjection(
        projections = Map("node" -> ident("node"))
      )
    )
  }

  test("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")

    val result = query.toString

    val expectation =
      """PlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b1)),OUTGOING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b1)),Set(),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(r -> Identifier(r), a1 -> Identifier(a1)),QueryShuffle(List(),None,Some(SignedDecimalIntegerLiteral(1)))),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(r), IdName(a1)),Selections(Set()),List(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b2)),INCOMING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b2)),Set(IdName(r), IdName(a1)),Selections(Set()),List(),Set(),Set())),Set(),Set()),RegularQueryProjection(Map(a1 -> Identifier(a1), r -> Identifier(r), b2 -> Identifier(b2)),QueryShuffle(List(),None,None)),None)))""".stripMargin

    result should equal(expectation)
  }

  // scalastyle:off
  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2")

    val result = query.toString

    val expectation =
      """PlannerQuery(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a1),IdName(b1)),OUTGOING,List(),SimplePatternLength)),Set(IdName(a1), IdName(b1)),Set(),Selections(Set()),List(),Set(),Set()),RegularQueryProjection(Map(r -> Identifier(r), a1 -> Identifier(a1)),QueryShuffle(List(),None,Some(SignedDecimalIntegerLiteral(1)))),Some(PlannerQuery(QueryGraph(Set(),Set(),Set(IdName(r), IdName(a1)),Selections(Set()),List(QueryGraph(Set(PatternRelationship(IdName(r),(IdName(a2),IdName(b2)),INCOMING,List(),SimplePatternLength)),Set(IdName(a2), IdName(b2)),Set(IdName(r), IdName(a1)),Selections(Set(Predicate(Set(IdName(a1), IdName(a2)),Equals(Identifier(a1),Identifier(a2))))),List(),Set(),Set())),Set(),Set()),RegularQueryProjection(Map(a1 -> Identifier(a1), r -> Identifier(r), b2 -> Identifier(b2), a2 -> Identifier(a2)),QueryShuffle(List(),None,None)),None)))""".stripMargin

    result should equal(expectation)
  }
  // scalastyle:on

  test("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")

    query.graph.patternNodes should equal(Set(IdName("a")))
    query.graph.patternRelationships should equal(Set.empty)

    query.horizon should equal(RegularQueryProjection(
      projections = Map(
        "x" -> FunctionInvocation(FunctionName("coalesce")_, distinct = false, Vector(ident("b"), ident("c")))(pos)
      )
    ))

    val optionalMatch1 :: optionalMatch2 :: Nil = query.graph.optionalMatches
    optionalMatch1.argumentIds should equal (Set(IdName("a")))
    optionalMatch1.patternNodes should equal (Set(IdName("a"), IdName("b")))


    optionalMatch2.argumentIds should equal (Set(IdName("a")))
    optionalMatch2.patternNodes should equal (Set(IdName("a"), IdName("c")))

    val tail = query.tail.get
    tail.graph.argumentIds should equal(Set(IdName("x")))
    tail.graph.patternNodes should equal(Set(IdName("x"), IdName("d")))

    tail.graph.optionalMatches should be (empty)
  }

  test("START n=node:nodes(name = \"A\") RETURN n") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("START n=node:nodes(name = \"A\") RETURN n")

    val hint: LegacyIndexHint = NodeByIdentifiedIndex(ident("n"), "nodes", "name", StringLiteral("A")_)_

    query.graph.hints should equal(Set(hint))
    query.tail should equal(None)
  }

  test("START n=node:nodes(\"name:A\") RETURN n") {
    val UnionQuery(query :: Nil, _, _) =
      buildPlannerQuery("START n=node:nodes(\"name:A\") RETURN n")

    val hint: LegacyIndexHint = NodeByIndexQuery(ident("n"), "nodes", StringLiteral("name:A")_)_

    query.graph.hints should equal(Set(hint))
    query.tail should equal(None)
  }

  def relType(name: String): RelTypeName = RelTypeName(name)_
}
