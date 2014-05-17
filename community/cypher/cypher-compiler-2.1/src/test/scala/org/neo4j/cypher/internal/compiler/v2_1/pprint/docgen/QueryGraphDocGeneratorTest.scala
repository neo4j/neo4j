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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{VarPatternLength, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{HasLabels, LabelName, AstConstructionTestSupport, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.graphdb.Direction

class QueryGraphDocGeneratorTest extends CypherFunSuite with AstConstructionTestSupport {
  object docGen extends NestedDocGenerator[Any] {
    val instance = QueryGraphDocGenerator orElse PlannerDocGenerator orElse ScalaDocGenerator orElse ToStringDocGenerator
  }

  private val rel1 = PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(), SimplePatternLength)
  private val rel2 = PatternRelationship(IdName("r2"), (IdName("b"), IdName("a")), Direction.INCOMING, Seq(RelTypeName("X")(null)), SimplePatternLength)
  private val rel3 = PatternRelationship(IdName("r3"), (IdName("c"), IdName("d")), Direction.OUTGOING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(1, None))
  private val rel4 = PatternRelationship(IdName("r4"), (IdName("d"), IdName("c")), Direction.INCOMING, Seq(RelTypeName("X")(null), RelTypeName("Y")(null)), VarPatternLength(0, Some(2)))

  test("render id names") {
    render(IdName("a")) should equal("a")
  }

  test("render rel type names") {
    render(RelTypeName("X")(null)) should equal("X")
  }

  test("render pattern rels") {
    render(rel1) should equal("(a)-[r1]->(b)")
    render(rel2) should equal("(b)<-[r2:X]-(a)")
    render(rel3) should equal("(c)-[r3:X|:Y*1..]->(d)")
    render(rel4) should equal("(d)<-[r4:X|:Y*0..2]-(c)")
  }

  test("render empty query graphs") {
    render(QueryGraph.empty) should equal("GIVEN *")
  }

  test("renders query graph arguments") {
    render(QueryGraph(argumentIds = Set(IdName("a")))) should equal("GIVEN a")
    render(QueryGraph(argumentIds = Set(IdName("a"), IdName("b")))) should equal("GIVEN a, b")
  }

  test("renders query graph nodes") {
    render(QueryGraph(patternNodes = Set(IdName("a"), IdName("b")))) should equal("GIVEN * MATCH (a), (b)")
  }

  test("renders query graph rels") {
    render(QueryGraph(
      patternNodes = Set(IdName("a"), IdName("b")),
      patternRelationships = Set(rel1)
    )) should equal("GIVEN * MATCH (a), (b), (a)-[r1]->(b)")
  }

  test("renders query graph selections") {
    render(QueryGraph(
      patternNodes = Set(IdName("a")),
      selections = Selections( predicates = Set(Predicate(Set(IdName("a")), HasLabels(ident("a"), Seq(LabelName("Person")_))_)))
    )) should equal("GIVEN * MATCH (a) WHERE Predicate[a](HasLabels(Identifier(\"a\"), LabelName(\"Person\") ⸬ nil))")
  }

  test("renders optional query graphs") {
    render(QueryGraph(
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(IdName("a")))
      )
    ))  should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a) }")
  }

  test("renders multiple optional query graphs") {
    render(QueryGraph(
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(IdName("a"))),
        QueryGraph(patternNodes = Set(IdName("b")))
      )
    ))  should equal("GIVEN * OPTIONAL { GIVEN * MATCH (a), GIVEN * MATCH (b) }")
  }

  private def render(v: Any) =
    pformat(v, formatter = DocFormatters.defaultLineFormatter)(docGen.docGen)
}
