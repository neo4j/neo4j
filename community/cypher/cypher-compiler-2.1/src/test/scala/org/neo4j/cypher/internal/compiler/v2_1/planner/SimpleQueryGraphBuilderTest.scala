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
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

class SimpleQueryGraphBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  // TODO: we may want to have normalized queries instead than simply parse queries
  val builder = new SimpleQueryGraphBuilder

  def buildQueryGraph(query: String): QueryGraph = {
    val ast = parser.parse(query).asInstanceOf[Query]
    builder.produce(ast)
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
      "n" -> Identifier("n")_
    ))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:Awesome:Foo RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Awesome:Foo RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> HasLabels(Identifier("n")_, Seq(LabelName("Awesome")()_))_,
      Set(IdName("n")) -> HasLabels(Identifier("n")_, Seq(LabelName("Foo")()_))_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("match n where n:X OR n:Y return n") {
    val qg = buildQueryGraph("match n where n:X OR n:Y return n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> Or(
        HasLabels(Identifier("n")_, Seq(LabelName("X")()_))_,
        HasLabels(Identifier("n")_, Seq(LabelName("Y")()_))_
      )_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> Or(
        HasLabels(Identifier("n")_, Seq(LabelName("X")()_))_,
        And(
          HasLabels(Identifier("n")_, Seq(LabelName("A")()_))_,
          HasLabels(Identifier("n")_, Seq(LabelName("B")()_))_
        )_
      )_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) = 42 RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> Equals(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")_
      )_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) IN [42, 43] RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) IN [42, 43] RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> In(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_))_
      )_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:Label AND id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Label AND id(n) = 42 RETURN n")
    qg.projections should equal(Map[String, Identifier](
      "n" -> Identifier("n")_
    ))

    qg.selections should equal(Selections(Set(
      Set(IdName("n")) -> HasLabels(Identifier("n")_, Seq(LabelName("Label")()_))_,
      Set(IdName("n")) -> Equals(
        FunctionInvocation(FunctionName("id")_, distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")_
      )_
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("match p = (a) return p") {
    val qg = buildQueryGraph("match p = (a) return p")
    qg.namedPaths should equal(Set(NamedNodePath("p", "a")))
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set[IdName]("a"))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "p" -> Identifier("p")_
    ))
    qg.coveredIds should equal(Set[IdName]("a", "p"))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    val qg = buildQueryGraph("match p = (a)-[r]->(b) return a,r")
    qg.namedPaths should equal(Set(NamedRelPath("p", Seq(patternRel))))
    qg.patternRelationships should equal(Set(patternRel))
    qg.patternNodes should equal(Set[IdName]("a", "b"))
    qg.selections should equal(Selections(Set.empty))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
    qg.coveredIds should equal(Set[IdName]("a", "r", "b", "p"))
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
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b", "r2").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
  }

  test("match (a), (b)-[r:Type]-(c) where b:Label return a,r") {
    val qg = buildQueryGraph("match (a), (b)-[r:Type]-(c) where b:Label return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("b"), IdName("c")), Direction.BOTH, Seq(relType("Type")), SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(Set(
      Set(IdName("b")) -> HasLabels(Identifier("b")_, Seq(LabelName("Label")()_))_
    )))
    qg.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "r" -> Identifier("r")_
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "c").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r1", "b", "r2", "c").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a").map(IdName(_)))
    qg.argumentIds should equal(Set())

    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set(IdName("a")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches.isEmpty should be(true)
    optMatchQG.coveredIds should equal(Set("a").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    qg.argumentIds should equal(Set())


    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches.isEmpty should be(true)
    optMatchQG.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
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
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    qg.argumentIds should equal(Set())

    val optMatchQG = qg.optionalMatches.head
    optMatchQG.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches.isEmpty should be(true)
    optMatchQG.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    optMatchQG.argumentIds should equal(Set(IdName("a")))
    optMatchQG.projections should equal(Map[String, Identifier](
      "a" -> Identifier("a")_,
      "b" -> Identifier("b")_,
      "r" -> Identifier("r")_
    ))
  }

  def relType(name: String): RelTypeName = RelTypeName(name)(None)_
}
