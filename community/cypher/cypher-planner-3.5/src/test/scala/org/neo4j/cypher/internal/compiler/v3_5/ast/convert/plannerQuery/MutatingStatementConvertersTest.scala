/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.convert.plannerQuery

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5._
import org.opencypher.v9_0.expressions.SemanticDirection.OUTGOING
import org.opencypher.v9_0.expressions._

class MutatingStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("setting a node property: MATCH (n) SET n.prop = 42 RETURN n") {
    val query = buildPlannerQuery("MATCH (n) SET n.prop = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(List(
      SetNodePropertyPattern("n", PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("42")(pos))
    ))
  }

  test("removing a node property should look like setting a property to null") {
    val query = buildPlannerQuery("MATCH (n) REMOVE n.prop RETURN n")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(List(
      SetNodePropertyPattern("n", PropertyKeyName("prop")(pos), Null()(pos))
    ))
  }

  test("setting a relationship property: MATCH (a)-[r]->(b) SET r.prop = 42 RETURN r") {
    val query = buildPlannerQuery("MATCH (a)-[r]->(b) SET r.prop = 42 RETURN r")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("r" -> varFor("r"))
    ))

    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, List(), SimplePatternLength)
    ))
    query.queryGraph.mutatingPatterns should equal(List(
      SetRelationshipPropertyPattern("r", PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("42")(pos))
    ))
  }

  test("removing a relationship property should look like setting a property to null") {
    val query = buildPlannerQuery("MATCH (a)-[r]->(b) REMOVE r.prop RETURN r")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("r" -> varFor("r"))
    ))

    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship("r", ("a", "b"), OUTGOING, List(), SimplePatternLength)
    ))
    query.queryGraph.mutatingPatterns should equal(List(
      SetRelationshipPropertyPattern("r", PropertyKeyName("prop")(pos), Null()(pos))
    ))
  }

  test("Query with single CREATE clause") {
    val query = buildPlannerQuery("CREATE (a), (b), (a)-[r:X]->(b) RETURN a, r, b")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("a" -> varFor("a"), "r" -> varFor("r"), "b" -> varFor("b"))
    ))

    query.queryGraph.mutatingPatterns should equal(Seq(
      CreatePattern(
        nodes("a", "b"),
        relationship("r", "a", "X", "b"))
    ))

    query.queryGraph.containsReads should be (false)
  }

  test("Read write and read again") {
    val query = buildPlannerQuery("MATCH (n) CREATE (m) WITH * MATCH (o) RETURN *")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n"), "m" -> varFor("m"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(Seq(CreatePattern(nodes("m"), Nil)))

    val next = query.tail.get

    next.queryGraph.patternNodes should equal(Set("o"))
    next.queryGraph.readOnly should be(true)
  }

  test("Unwind, read write and read again") {
    val query = buildPlannerQuery("UNWIND [1] as i MATCH (n) CREATE (m) WITH * MATCH (o) RETURN *")
    query.horizon should equal(UnwindProjection("i", ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos)))
    query.queryGraph shouldBe 'isEmpty

    val second = query.tail.get

    second.queryGraph.patternNodes should equal(Set("n"))
    second.queryGraph.mutatingPatterns should equal(IndexedSeq(CreatePattern(nodes("m"), Nil)))

    val third = second.tail.get

    third.queryGraph.patternNodes should equal(Set("o"))
    third.queryGraph.readOnly should be(true)
  }

  test("Foreach with create node") {
    val query = buildPlannerQuery("FOREACH (i in [1] | CREATE (a))")
    query.queryGraph shouldBe 'isEmpty
    val second = query.tail.get
    second.queryGraph.mutatingPatterns should equal(
      Seq(ForeachPattern("i",
          ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos),
          RegularPlannerQuery(QueryGraph(Set.empty, Set.empty, Set("i"),
                                         Selections(Set.empty), Vector.empty, Seq.empty, Set.empty,
                                         IndexedSeq(CreatePattern(nodes("a"), Nil))),
                              RegularQueryProjection(Map("i" -> Variable("i")(pos))), None)))
    )
  }

  private def nodes(names: String*) = {
    names.map(name => CreateNode(name, Seq.empty, None))
  }

  private def relationship(name: String, startNode: String, relType:String, endNode: String) = {
    List(CreateRelationship(name, startNode, RelTypeName(relType)(pos), endNode, SemanticDirection.OUTGOING, None))
  }
}
