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

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.{inSequence, bottomUp}
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters.{inlineProjections, namePatternPredicates, nameVarLengthRelationships}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

class SimpleQueryGraphBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val builder = new SimpleQueryGraphBuilder

  val nIdent: Identifier = Identifier("n")_
  val A: LabelName = LabelName("A")_
  val B: LabelName = LabelName("B")_
  val X: LabelName = LabelName("X")_
  val Y: LabelName = LabelName("Y")_
  val lit42: SignedIntegerLiteral = SignedIntegerLiteral("42")_
  val lit43: SignedIntegerLiteral = SignedIntegerLiteral("43")_


  def buildQueryGraph(query: String, normalize:Boolean = false): QueryGraph = {
    val ast = parser.parse(query)

    val rewrittenAst: Statement = if (normalize) {
      val step1 = astRewriter.rewrite(query, ast)._1
      val step2 = step1.rewrite(bottomUp(inSequence(nameVarLengthRelationships, namePatternPredicates))).asInstanceOf[Statement]
      val step3 = inlineProjections(step2)
      step3
    } else {
      ast
    }

    builder.produce(rewrittenAst.asInstanceOf[Query])
  }

  test("RETURN 42") {
    val qg = buildQueryGraph("RETURN 42")
    qg.projections should equal(Map[String, Literal]("42" -> SignedIntegerLiteral("42")_))
  }

  test("RETURN 42, 'foo'") {
    val qg = buildQueryGraph("RETURN 42, 'foo'")
    qg.projections should equal(Map[String, Literal](
      "42" -> SignedIntegerLiteral("42")_,
      "'foo'" -> StringLiteral("foo")_
    ))
  }

  test("match n return n") {
    val qg = buildQueryGraph("match n return n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:A:B RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:A:B RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(B))_
    ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("match n where n:X OR n:Y return n") {
    val qg = buildQueryGraph("match n where n:X OR n:Y return n", normalize = true)
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(List(
        HasLabels(nIdent, Seq(X))(pos),
        HasLabels(nIdent, Seq(Y))(pos)
      ))_
    ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n", normalize = true)
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Ors(List(
        HasLabels(nIdent, Seq(X))(pos),
        Ands(List(
          HasLabels(nIdent, Seq(A))(pos),
          HasLabels(nIdent, Seq(B))(pos)
        ))(pos)))_
      ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) = 42 RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Equals(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")_
      )_
    ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) IN [42, 43] RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) IN [42, 43] RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(lit42, lit43))_
      )_
    ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:A AND id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:A AND id(n) = 42 RETURN n", normalize = true)
    qg.projections should equal(Map[String, Identifier](
      "n" -> nIdent
    ))

    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_),
      Predicate(Set(IdName("n")), Equals(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")_
      )_
    ))))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("match p = (a) return p") {
    val qg = buildQueryGraph("match p = (a) return p", normalize = true)
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set[IdName]("a"))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Expression](
      "p" -> PathExpression(NodePathStep(Identifier("a")_, NilPathStep))_
    ))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    val qg = buildQueryGraph("match p = (a)-[r]->(b) return a,r", normalize = true)
    qg.patternRelationships should equal(Set(patternRel))
    qg.patternNodes should equal(Set[IdName]("a", "b"))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_,
      "b" -> Identifier("b")_
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(a) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val qg = buildQueryGraph("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("n"), IdName("c")), Direction.BOTH, Seq(relType("Type")), SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("n"), IdName("c")))
    qg.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), HasLabels(nIdent, Seq(A))_)
    )))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type|Foo]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c") {
    val qg = buildQueryGraph("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(relType("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq(relType("FRIEND")), VarPatternLength(0, Some(1)))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "c" -> Identifier("c")_
    ))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*3..]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*5]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, VarPatternLength(1, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
  }

  test("optional match (a) return a") {
    val qg = buildQueryGraph("optional match (a) return a")
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set())
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_
    ))

    qg.optionalMatches.size should equal(1)
    qg.argumentIds should equal(Set())

    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set(IdName("a")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("optional match (a)-[r]->(b) return a,b,r") {
    val qg = buildQueryGraph("optional match (a)-[r]->(b) return a,b,r")
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set())
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    ))

    qg.optionalMatches.size should equal(1)
    qg.argumentIds should equal(Set())


    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be (empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match a optional match (a)-[r]->(b) return a,b,r") {
    val qg = buildQueryGraph("match a optional match (a)-[r]->(b) return a,b,r")
    qg.patternNodes should equal(Set(IdName("a")))
    qg.patternRelationships should equal(Set())
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    ))
    qg.optionalMatches.size should equal(1)
    qg.argumentIds should equal(Set())

    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set(IdName("a")))
    optMatchQG.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    ))
  }

  test("match a where (a)-->() return a") {
    // Given
    val qg = buildQueryGraph("match a where (a)-->() return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED17"
    val nodeName = "  UNNAMED21"
    val exp: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val predicate= Predicate(Set(IdName("a")), exp)
    val subQueryTable = Map(exp ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))).addCoveredIdsAsProjections())

    val selections = Selections(Set(predicate))

    qg.selections should equal(selections)
    qg.patternNodes should equal(Set(IdName("a")))
    qg.subQueriesLookupTable should equal(subQueryTable)
  }

  test("match n return n.prop order by n.prop2 DESC") {
    // Given
    val qg = buildQueryGraph("match n return n.prop order by n.prop2 DESC", normalize = true)

    // Then inner pattern query graph
    qg.selections should equal(Selections())
    qg.patternNodes should equal(Set(IdName("n")))
    qg.subQueriesLookupTable should be(empty)
    val sortItem: DescSortItem = DescSortItem(Property(Identifier("n")_, PropertyKeyName("prop2")_)_)_
    qg.sortItems should equal(Seq(sortItem))
  }

  test("MATCH (a) WITH 1 as b RETURN b") {
    val qg = buildQueryGraph("MATCH (a) WITH 1 as b RETURN b", normalize = true)
    qg.patternNodes should equal(Set(IdName("a")))
    qg.projections should equal(Map[String, Expression]("b" -> SignedIntegerLiteral("1")_))
    qg.tail should equal(None)
  }

  test("WITH 1 as b RETURN b") {
    val qg = buildQueryGraph("WITH 1 as b RETURN b", normalize = true)

    qg.projections should equal(Map[String, Expression]("b" -> SignedIntegerLiteral("1")_))
    qg.tail should equal(None)
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val qg = buildQueryGraph("MATCH (a) WITH a WHERE TRUE RETURN a")
    qg.patternNodes should equal(Set(IdName("a")))
    qg.projections should equal(Map[String, Expression]("a" -> Identifier("a")_))

    val tail = qg.tail.get
    tail.projections should equal(Map[String, Expression]("a" -> Identifier("a")_))
    tail.selections should equal(Selections(Set(Predicate(Set.empty, True()_))))
  }

  test("match a where a.prop = 42 OR (a)-->() return a") {
    // Given
    val qg = buildQueryGraph("match a where a.prop = 42 OR (a)-->() return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED32"
    val nodeName = "  UNNAMED36"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = Equals(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      SignedIntegerLiteral("42")_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(List(exp1, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))).addCoveredIdsAsProjections())

    val selections = Selections(Set(orPredicate))

    qg.selections should equal(selections)
    qg.patternNodes should equal(Set(IdName("a")))
    qg.subQueriesLookupTable should equal(subQueriesTable)
  }

  test("match a where (a)-->() OR a.prop = 42 return a") {
    // Given
    val qg = buildQueryGraph("match a where (a)-->() OR a.prop = 42 return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED17"
    val nodeName = "  UNNAMED21"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = Equals(
      Property(Identifier("a") _, PropertyKeyName("prop")_)_,
      SignedIntegerLiteral("42") _
    ) _
    val orPredicate = Predicate(Set(IdName("a")), Ors(List(exp1, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))).addCoveredIdsAsProjections())

    val selections = Selections(Set(orPredicate))

    qg.selections should equal(selections)
    qg.patternNodes should equal(Set(IdName("a")))
    qg.subQueriesLookupTable should equal(subQueriesTable)
  }

  test("match a where a.prop = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val qg = buildQueryGraph("match a where a.prop = 21 OR (a)-->() OR a.prop = 42 return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED32"
    val nodeName = "  UNNAMED36"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = Equals(
      Property(Identifier("a") _, PropertyKeyName("prop")_)_,
      SignedIntegerLiteral("42")_
    )_
    val exp3: Expression = Equals(
      Property(Identifier("a") _, PropertyKeyName("prop")_)_,
      SignedIntegerLiteral("21")_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(List(exp1, exp3, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))).addCoveredIdsAsProjections())

    val selections = Selections(Set(orPredicate))

    qg.selections should equal(selections)
    qg.patternNodes should equal(Set(IdName("a")))
    qg.subQueriesLookupTable should equal(subQueriesTable)
  }

  test("match n return n limit 10") {
    // Given
    val qg = buildQueryGraph("match n return n limit 10", normalize = true)

    // Then inner pattern query graph
    qg.selections should equal(Selections())
    qg.patternNodes should equal(Set(IdName("n")))
    qg.subQueriesLookupTable should be(empty)
    qg.sortItems should equal(Seq.empty)
    qg.limit should equal(Some(UnsignedIntegerLiteral("10")(pos)))
    qg.skip should equal(None)
  }

  test("match n return n skip 10") {
    // Given
    val qg = buildQueryGraph("match n return n skip 10", normalize = true)

    // Then inner pattern query graph
    qg.selections should equal(Selections())
    qg.patternNodes should equal(Set(IdName("n")))
    qg.subQueriesLookupTable should be(empty)
    qg.sortItems should equal(Seq.empty)
    qg.limit should equal(None)
    qg.skip should equal(Some(UnsignedIntegerLiteral("10")(pos)))
  }

  test("match (a) with * return a") {
    val qg = buildQueryGraph("match (a) with * return a")
    qg.patternNodes should equal(Set(IdName("a")))
    qg.projections should equal(Map[String, Expression]("a" -> Identifier("a")_))
    qg.tail should equal(None)
  }

  test("should fail with one or more pattern expression in or") {
    evaluating(buildQueryGraph("match (a) where (a)-->() OR (a)-[:X]->() return a", normalize = true)) should produce[CantHandleQueryException]
    evaluating(buildQueryGraph("match (a) where not (a)-->() OR (a)-[:X]->() return a", normalize = true)) should produce[CantHandleQueryException]
    evaluating(buildQueryGraph("match (a) where (a)-->() OR not (a)-[:X]->() return a", normalize = true)) should produce[CantHandleQueryException]
    evaluating(buildQueryGraph("match (a) where not (a)-->() OR not (a)-[:X]->() return a", normalize = true)) should produce[CantHandleQueryException]
    evaluating(buildQueryGraph("match (a) where (a)-->() OR id(a) = 12 OR (a)-[:X]->() return a", normalize = true)) should produce[CantHandleQueryException]
  }

  def relType(name: String): RelTypeName = RelTypeName(name)_
}
