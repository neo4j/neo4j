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

import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, Selections, SimpleQueryGraphBuilder}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{PatternRelationship, IdName}
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
  }

  test("match (a)-[r]->(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r]->(b) return a,r")
    qg.patternRelationships should equal(
      Set(PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.OUTGOING, Seq.empty)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos),
      "b" -> Identifier("b")(pos)
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val qg = buildQueryGraph("match (a)-[r]->(b)-[r2]->(a) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.OUTGOING, Seq.empty)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val qg = buildQueryGraph("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq.empty),
      PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty)))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List()))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  test("match (a), (b)-[r:Type]-(c) where b:Label return a,r") {
    val qg = buildQueryGraph("match (a), (b)-[r:Type]-(c) where b:Label return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("b"), IdName("c")), Direction.BOTH, Seq(relType("Type")))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b"), IdName("c")))
    qg.selections should equal(Selections(List(
      Set(IdName("b")) -> HasLabels(Identifier("b")(pos), Seq(LabelName("Label")()(pos)))(pos)
    )))
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val qg = buildQueryGraph("match (a)-[r:Type|Foo]-(b) return a,r")
    qg.patternRelationships should equal(Set(
      PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq(relType("Type"), relType("Foo")))))
    qg.patternNodes should equal(Set(IdName("a"), IdName("b")))
    qg.selections should equal(Selections())
    qg.projections should equal(Map(
      "a" -> Identifier("a")(pos),
      "r" -> Identifier("r")(pos)
    ))
  }

  def relType(name: String): RelTypeName = RelTypeName(name)(None)(pos)
}
