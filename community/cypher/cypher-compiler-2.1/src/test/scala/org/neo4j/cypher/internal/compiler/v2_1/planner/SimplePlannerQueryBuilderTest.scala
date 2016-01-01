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
import org.neo4j.cypher.internal.compiler.v2_1.InputPosition
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._


class SimplePlannerQueryBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val builder = new SimplePlannerQueryBuilder

  val nIdent: Identifier = Identifier("n")_
  val A: LabelName = LabelName("A")_
  val B: LabelName = LabelName("B")_
  val X: LabelName = LabelName("X")_
  val Y: LabelName = LabelName("Y")_
  val lit42: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
  val lit43: SignedIntegerLiteral = SignedDecimalIntegerLiteral("43")_

  val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val property: Expression = Property( Identifier( "n" ) _, PropertyKeyName( "prop" ) _ ) _

  def buildPlannerQuery(query: String, normalize:Boolean = false): QueryPlanInput = {
    val ast = parser.parse(query)

    val rewrittenAst: Statement = if (normalize) {
      Planner.rewriteStatement(astRewriter.rewrite(query, ast)._1)
    } else {
      ast
    }

    builder.produce(rewrittenAst.asInstanceOf[Query])
  }

  test("RETURN 42") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("RETURN 42")
    query.horizon should equal(RegularQueryProjection(Map("42" -> SignedDecimalIntegerLiteral("42")_)))
  }

  test("RETURN 42, 'foo'") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("RETURN 42, 'foo'")
    query.horizon should equal(RegularQueryProjection(Map(
      "42" -> SignedDecimalIntegerLiteral("42")_,
      "'foo'" -> StringLiteral("foo")_
    )))
  }

  test("match n return n") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match n return n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:A:B RETURN n") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n WHERE n:A:B RETURN n")
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match n where n:X OR n:Y return n", normalize = true)
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n", normalize = true)
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")),
        Ors(Set(
          HasLabels(nIdent, Seq(X))(pos),
          HasLabels(nIdent, Seq(B))(pos)
        ))(pos)
      ),
      Predicate(Set(IdName("n")),
        Ors(Set(
          HasLabels(nIdent, Seq(X))(pos),
          HasLabels(nIdent, Seq(A))(pos)
        ))(pos)
      )
    )))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) = 42 RETURN n") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n WHERE id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(Map(
      "n" -> nIdent
    )))

    query.graph.selections should equal(Selections(Set(
      Predicate(Set(IdName("n")), Equals(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedDecimalIntegerLiteral("42")_
      )_
    ))))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) IN [42, 43] RETURN n") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n WHERE id(n) IN [42, 43] RETURN n")
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n WHERE n:A AND id(n) = 42 RETURN n", normalize = true)
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match p = (a) return p", normalize = true)
    query.graph.patternRelationships should equal(Set())
    query.graph.patternNodes should equal(Set[IdName]("a"))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression](
      "p" -> PathExpression(NodePathStep(Identifier("a")_, NilPathStep))_
    )))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match p = (a)-[r]->(b) return a,r", normalize = true)
    query.graph.patternRelationships should equal(Set(patternRel))
    query.graph.patternNodes should equal(Set[IdName]("a", "b"))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_,
      "b" -> Identifier("b")_
    )))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r]->(b)-[r2]->(a) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("n"), IdName("c")), Direction.BOTH, Seq(relType("Type")), SimplePatternLength)))
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r:Type|Foo]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r:Type*]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r1:CONTAINS*0..1]->b-[r2:FRIEND*0..1]->c return a,b,c")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(relType("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq(relType("FRIEND")), VarPatternLength(0, Some(1)))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "c" -> Identifier("c")_
    )))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r:Type*3..]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)-[r:Type*5]-(b) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    query.graph.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, VarPatternLength(1, None))))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    query.graph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(Map(
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    )))
  }

  test("optional match (a) return a") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("optional match (a) return a")
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("optional match (a)-[r]->(b) return a,b,r")
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
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be (empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match a optional match (a)-[r]->(b) return a,b,r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match a optional match (a)-[r]->(b) return a,b,r")
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
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set(IdName("a")))
  }

  test("match a where (a)-->() return a") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("match a where (a)-->() return a", normalize = true)

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
        argumentIds = Set(IdName("a"))))

    val selections = Selections(Set(predicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
    lookupTable should equal(subQueryTable)
  }

  test("match n return n.prop order by n.prop2 DESC") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("match n return n.prop order by n.prop2 DESC", normalize = true)

    // Then inner pattern query graph
    query.graph.selections should equal(Selections())
    query.graph.patternNodes should equal(Set(IdName("n")))
    lookupTable should be(empty)
    val sortItem: DescSortItem = DescSortItem(Property(Identifier("n")_, PropertyKeyName("prop2")_)_)_

    query.horizon should equal(
      RegularQueryProjection(
        Map("n.prop" -> property),
        QueryShuffle(
          sortItems = Seq(sortItem)))
    )
  }

  test("MATCH (a) WITH 1 as b RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (a) WITH 1 as b RETURN b", normalize = true)
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(None)
  }

  test("WITH 1 as b RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("WITH 1 as b RETURN b", normalize = true)

    query.horizon should equal(RegularQueryProjection(Map("b" -> SignedDecimalIntegerLiteral("1")_)))
    query.tail should equal(None)
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (a) WITH a WHERE TRUE RETURN a", normalize = true)
    query.tail should be(empty)
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map("a" -> Identifier("a")_)))
    query.graph.selections.predicates should equal(
      Set(Predicate(Set.empty, True()_))
    )
  }

  test("match a where a.prop = 42 OR (a)-->() return a") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("match a where a.prop = 42 OR (a)-->() return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED32"
    val nodeName = "  UNNAMED36"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))))

    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
    lookupTable should equal(subQueriesTable)
  }

  test("match a where (a)-->() OR a.prop = 42 return a") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("match a where (a)-->() OR a.prop = 42 return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED17"
    val nodeName = "  UNNAMED21"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    ) _
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))))

    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
    lookupTable should equal(subQueriesTable)
  }

  test("match a where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("match a where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a", normalize = true)

    // Then inner pattern query graph
    val relName = "  UNNAMED33"
    val nodeName = "  UNNAMED37"
    val exp1: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
      RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, Seq.empty, None, None, Direction.OUTGOING) _,
      NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
    ) _) _)
    val relationship = PatternRelationship(IdName(relName), (IdName("a"), IdName(nodeName)), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val exp2: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val exp3: Expression = In(
      Property(Identifier("a")_, PropertyKeyName("prop2")_)_,
      Collection(Seq(SignedDecimalIntegerLiteral("21")_))_
    )_
    val orPredicate = Predicate(Set(IdName("a")), Ors(Set(exp1, exp3, exp2))_)
    val subQueriesTable = Map(exp1 ->
      QueryGraph(
        patternRelationships = Set(relationship),
        patternNodes = Set("a", nodeName),
        argumentIds = Set(IdName("a"))))

    val selections = Selections(Set(orPredicate))

    query.graph.selections should equal(selections)
    query.graph.patternNodes should equal(Set(IdName("a")))
    lookupTable should equal(subQueriesTable)
  }

  test("match n return n limit 10") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match n return n limit 10", normalize = true)

    // Then inner pattern query graph
    query.graph.selections should equal(Selections())
    query.graph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->ident("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(UnsignedDecimalIntegerLiteral("10")(pos)))))
  }

  test("match n return n skip 10") {
    // Given
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match n return n skip 10", normalize = true)

    // Then inner pattern query graph
    query.graph.selections should equal(Selections())
    query.graph.patternNodes should equal(Set(IdName("n")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n"->ident("n")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = Some(UnsignedDecimalIntegerLiteral("10")(pos)),
          limit = None)))
  }

  test("match (a) with * return a") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("match (a) with * return a")
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(RegularQueryProjection(Map[String, Expression]("a" -> Identifier("a")_)))
    query.tail should equal(None)
  }

  test("MATCH a WITH a LIMIT 1 MATCH a-[r]->b RETURN a, b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH a WITH a LIMIT 1 MATCH a-[r]->b RETURN a, b", normalize = true)
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
          limit = Some(UnsignedDecimalIntegerLiteral("1")(pos)))))

    query.tail should not be empty
    query.tail.get.graph should equal(
      QueryGraph
      .empty
      .addArgumentId(Seq("a"))
      .addPatternNodes("a", "b")
      .addPatternRel(patternRel)
    )
  }

  test("optional match (a:Foo) with a match (a)-[r]->(b) return a") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("optional match (a:Foo) with a match (a)-[r]->(b) return a", normalize = true)

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
      .addArgumentId(Seq("a"))
      .addPatternNodes("a", "b")
      .addPatternRel(patternRel)
    )
  }

  test("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b", normalize = true)
    query.tail should not be empty
    query.graph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Identifier("a")_, Seq(LabelName("Start")(null)))_)
    ))
    query.graph.patternNodes should equal(Set(IdName("a")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("a" -> ident("a")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = Some(UnsignedDecimalIntegerLiteral("1")(pos)))))

    val tailQg = query.tail.get
    tailQg.graph.patternNodes should equal(Set(IdName("b")))
    tailQg.graph.patternRelationships should be(empty)
    tailQg.graph.selections.predicates should equal(Set(
      Predicate(
        Set(IdName("b"), IdName("a")),
        Equals(FunctionInvocation(FunctionName("id")_, Identifier("b")_)_, Property(Identifier("a")_, PropertyKeyName("prop")_)_)_
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> ident("b")),
        QueryShuffle()))
  }

  test("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b", normalize = true)
    query.tail should be(empty)
    query.graph.selections.predicates should equal(Set(
      Predicate(Set(IdName("a")), HasLabels(Identifier("a")_, Seq(LabelName("Start")(null)))_),
      Predicate(
        Set(IdName("b"), IdName("a")),
        Equals(FunctionInvocation(FunctionName("id")_, Identifier("b")_)_, Property(Identifier("a")_, PropertyKeyName("prop")_)_)_
      )
    ))
    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> ident("b")),
        QueryShuffle(
          sortItems = Seq.empty,
          skip = None,
          limit = None)))
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b", normalize = true)
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
        Equals(FunctionInvocation(FunctionName("id")_, Identifier("b")_)_, Identifier("property")_)_
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> ident("b")),
        QueryShuffle()))
  }

  test("MATCH n RETURN count(*)") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n RETURN count(*)")

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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH n RETURN n.prop, count(*)")

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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n")

    query.graph.hints should equal(Set[Hint](UsingIndexHint(ident("n"), LabelName("Awesome")_, ident("prop"))_))
  }

  test("MATCH shortestPath(a-[r]->b) RETURN r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH shortestPath(a-[r]->b) RETURN r")

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(None, PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH allShortestPaths(a-[r]->b) RETURN r") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH allShortestPaths(a-[r]->b) RETURN r")

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(None, PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength), single = false)(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH p = shortestPath(a-[r]->b) RETURN p") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _)  = buildPlannerQuery("MATCH p = shortestPath(a-[r]->b) RETURN p", normalize = true)

    query.graph.patternNodes should equal(Set(IdName("a"), IdName("b")))
    query.graph.shortestPathPatterns should equal(Set(
      ShortestPathPattern(Some(IdName("p")), PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null)
    ))
    query.tail should be(empty)
  }

  test("RETURN 1 as x UNION RETURN 2 as x") {
    val QueryPlanInput(query, _) = buildPlannerQuery("RETURN 1 as x UNION RETURN 2 as x")
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
    val QueryPlanInput(query, _) = buildPlannerQuery("RETURN 1 as x UNION ALL RETURN 2 as x UNION ALL RETURN 3 as x")
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
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("match n return distinct n")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("n" -> ident("n")),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match n with distinct n.prop as x return x") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("match n with distinct n.prop as x return x")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("x" -> property),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("match n with distinct * return x") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("match n with distinct * return x")
    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("n" -> ident("n")),
      aggregationExpressions = Map.empty
    ))

    query.graph.patternNodes should equal(Set(IdName("n")))
  }

  test("WITH DISTINCT 1 as b RETURN b") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("WITH DISTINCT 1 as b RETURN b", normalize = true)

    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("b" -> SignedDecimalIntegerLiteral("1") _),
      aggregationExpressions = Map.empty
    ))

    query.tail should not be empty
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN *") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner", normalize = true)

    query.graph.patternNodes should equal(Set(IdName("owner")))
      query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("owner" -> ident("owner")),
      aggregationExpressions = Map("collected" -> CountStar()(pos))
    ))

    val tailQuery = query.tail.get

    tailQuery.graph.patternNodes should be(empty)
    tailQuery.horizon should equal(RegularQueryProjection(Map("owner" -> ident("owner"))))
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS xyz WITH owner, xyz > 0 as collection WHERE (owner)--() RETURN owner") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), lookupTable) = buildPlannerQuery(
      """MATCH (owner)
        |WITH owner, COUNT(*) AS xyz
        |WITH owner, xyz > 0 as collection
        |WHERE (owner)--()
        |RETURN owner""".stripMargin, normalize = true)

    query.horizon should equal(AggregatingQueryProjection(
      groupingKeys = Map("owner" -> ident("owner")),
      aggregationExpressions = Map("xyz" -> CountStar()(pos))
    ))

    val tail1Query = query.tail.get

    tail1Query.horizon should equal(RegularQueryProjection(
      Map(
           // Removed by inliner since it never gets returned
           // "collection" -> GreaterThan(ident("xyz"), SignedDecimalIntegerLiteral("0") _) _,
          "owner" -> ident("owner"))
    ))
  }

  test("UNWIND [1,2,3] AS x RETURN x") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) = buildPlannerQuery("UNWIND [1,2,3] AS x RETURN x", normalize = true)

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

  val one = SignedDecimalIntegerLiteral("1")(pos)
  val two = SignedDecimalIntegerLiteral("2")(pos)
  val three = SignedDecimalIntegerLiteral("3")(pos)
  val collection = Collection(Seq(one, two, three))(pos)

  test("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) =
      buildPlannerQuery("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n", normalize = true)

    query.horizon should equal(UnwindProjection(
      IdName("x"), collection
    ))

    val tail = query.tail.get

    tail.graph.patternNodes should equal(Set(IdName("n")))
    val set: Set[Predicate] = Set(
      Predicate(Set(IdName("n"), IdName("x")), Equals(property, ident("x")) _))

    tail.graph.selections.predicates should equal(set)
    tail.horizon should equal(RegularQueryProjection(Map("n" -> ident("n"))))
  }

  test("MATCH n UNWIND n.prop as x RETURN x") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) =
      buildPlannerQuery("MATCH n UNWIND n.prop as x RETURN x", normalize = true)

    query.graph.patternNodes should equal(Set(IdName("n")))

    query.horizon should equal(UnwindProjection(
      IdName("x"),
      property
    ))

    val tail = query.tail.get
    tail.graph.patternNodes should equal(Set.empty)
    tail.horizon should equal(RegularQueryProjection(Map("x" -> ident("x"))))
  }

  test("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node") {
    val QueryPlanInput(UnionQuery(query :: Nil, _), _) =
      buildPlannerQuery("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node", normalize = true)

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

  def relType(name: String): RelTypeName = RelTypeName(name)_
}
