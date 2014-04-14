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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SimplePatternLength, VarPatternLength, PatternRelationship, IdName}
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, Selections, SimpleQueryGraphBuilder}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.graphdb.Direction

class SimpleQueryGraphBuilderTest extends CypherFunSuite {

  // TODO: we may want to have normalized queries instead than simply parse queries
  val parser = new CypherParser(mock[ParserMonitor])
  val pos = DummyPosition(0)

  val builder = new SimpleQueryGraphBuilder

  def buildQueryGraph(query: String): QueryGraph = {
    val ast = parser.parse(query).asInstanceOf[Query]
    builder.produce(ast)
  }

  test("RETURN 42") {
    val qg = buildQueryGraph("RETURN 42")
    qg.projections should equal(Map("42" -> SignedIntegerLiteral("42")(pos)))
  }

  test("RETURN 42, 'foo'") {
    val qg = buildQueryGraph("RETURN 42, 'foo'")
    qg.projections should equal(Map(
      "42" -> SignedIntegerLiteral("42")(pos),
      "'foo'" -> StringLiteral("foo")(pos)
    ))
  }

  test("match n return n") {
    val qg = buildQueryGraph("match n return n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:Awesome:Foo RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Awesome:Foo RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos),
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Foo")()(pos)))(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("match n where n:X OR n:Y return n") {
    val qg = buildQueryGraph("match n where n:X OR n:Y return n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(IdName("n")) -> Or(
        HasLabels(Identifier("n")(pos), Seq(LabelName("X")()(pos)))(pos),
        HasLabels(Identifier("n")(pos), Seq(LabelName("Y")()(pos)))(pos)
      )(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:X OR (n:A AND n:B) RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(IdName("n")) -> Or(
        HasLabels(Identifier("n")(pos), Seq(LabelName("X")()(pos)))(pos),
        And(
          HasLabels(Identifier("n")(pos), Seq(LabelName("A")()(pos)))(pos),
          HasLabels(Identifier("n")(pos), Seq(LabelName("B")()(pos)))(pos)
        )(pos)
      )(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) = 42 RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(List(
      Set(IdName("n")) -> Equals(
        FunctionInvocation(FunctionName("id")(pos), distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE id(n) IN [42, 43] RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE id(n) IN [42, 43] RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(List(
      Set(IdName("n")) -> In(
        FunctionInvocation(FunctionName("id")(pos), distinct = false, Vector(Identifier("n")(pos)))(pos),
        Collection(Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos)))(pos)
      )(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
  }

  test("MATCH n WHERE n:Label AND id(n) = 42 RETURN n") {
    val qg = buildQueryGraph("MATCH n WHERE n:Label AND id(n) = 42 RETURN n")
    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(List(
      Set(IdName("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Label")()(pos)))(pos),
      Set(IdName("n")) -> Equals(
        FunctionInvocation(FunctionName("id")(pos), distinct = false, Vector(Identifier("n")(pos)))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos)
    )))

    qg.patternNodes should equal(Set(IdName("n")))
    qg.coveredIds should equal(Set(IdName("n")))
  }

  test("match (a)-[r]->(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r]->(b) return a,r")
    qg.patternRelationships should equal(
      Set(PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos),
      "b" -> Identifier("b")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(a) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "r2").map(IdName(_)))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
  }

  test("match (a), (b)-[r:Type]-(c) where b:Label return a,r") {
    val qg = buildQueryGraph("match (a), (b)-[r:Type]-(c) where b:Label return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("b"), IdName("c")), Direction.BOTH, Seq(relType("Type")), SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List(
      Set(IdName("b")) -> HasLabels(Identifier("b")(pos), Seq(LabelName("Label")()(pos)))(pos)
    )))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "c").map(IdName(_)))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type|Foo]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type"), relType("Foo")), SimplePatternLength)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(1, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
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
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "b" -> Identifier("b")(pos),
      "c" -> Identifier("c")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r1", "b", "r2", "c").map(IdName(_)))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*3..]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength(3, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type*5]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type")), VarPatternLength.fixed(5))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty, VarPatternLength(1, None))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.coveredIds should equal(Set("a", "r", "b", "r2", "c").map(IdName(_)))
  }

  test("optional match (a) return a") {
    val qg = buildQueryGraph("optional match (a) return a")
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set())
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos)
    ))

    qg.optionalMatches.size should equal(1)
    qg.coveredIds should equal(Set("a").map(IdName(_)))
    qg.requiredIds should equal(Set())

    val optMatchqg = qg.optionalMatches.head
    optMatchqg.patternRelationships should equal(Set())
    optMatchqg.patternNodes should equal(Set(IdName("a")))
    optMatchqg.selections should equal(Selections(List()))
    optMatchqg.projections should equal(Map())
    optMatchqg.optionalMatches.isEmpty should be(true)
    optMatchqg.coveredIds should equal(Set("a").map(IdName(_)))
    optMatchqg.requiredIds should equal(Set())
  }

  test("optional match (a)-[r]->(b) return a,b,r") {
    val qg = buildQueryGraph("optional match (a)-[r]->(b) return a,b,r")
    qg.patternRelationships should equal(Set())
    qg.patternNodes should equal(Set())
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "b" -> Identifier("b")(pos),
      "r" -> Identifier("r")(pos)
    ))

    qg.optionalMatches.size should equal(1)
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    qg.requiredIds should equal(Set())


    val optMatchqg = qg.optionalMatches.head
    optMatchqg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchqg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchqg.selections should equal(Selections(List()))
    optMatchqg.projections should equal(Map())
    optMatchqg.optionalMatches.isEmpty should be(true)
    optMatchqg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    optMatchqg.requiredIds should equal(Set())
  }

  test("match a optional match (a)-[r]->(b) return a,b,r") {
    val qg = buildQueryGraph("match a optional match (a)-[r]->(b) return a,b,r")
    qg.patternNodes should equal(Set(IdName("a")))
    qg.patternRelationships should equal(Set())
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "b" -> Identifier("b")(pos),
      "r" -> Identifier("r")(pos)
    ))
    qg.optionalMatches.size should equal(1)
    qg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    qg.requiredIds should equal(Set())


    val optMatchqg = qg.optionalMatches.head
    optMatchqg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    optMatchqg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchqg.selections should equal(Selections(List()))
    optMatchqg.projections should equal(Map())
    optMatchqg.optionalMatches.isEmpty should be(true)
    optMatchqg.coveredIds should equal(Set("a", "r", "b").map(IdName(_)))
    optMatchqg.requiredIds should equal(Set(IdName("a")))
  }

  def relType(name: String): RelTypeName = RelTypeName(name)(None)(pos)
}
