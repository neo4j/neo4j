/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.AppendedClues
import org.scalatest.prop.TableDrivenPropertyChecks

class MutatingStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport with AppendedClues
    with TableDrivenPropertyChecks {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.GpmShortestPath
  )

  test("setting a node property: MATCH (n) SET n.prop = 42 RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) SET n.prop = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n")),
      isTerminating = true
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(List(
      SetNodePropertyPattern(varFor("n"), PropertyKeyName("prop")(pos), literalInt(42))
    ))
  }

  test("removing a node property should look like setting a property to null") {
    val query = buildSinglePlannerQuery("MATCH (n) REMOVE n.prop RETURN n")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n")),
      isTerminating = true
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(List(
      SetNodePropertyPattern(varFor("n"), PropertyKeyName("prop")(pos), nullLiteral)
    ))
  }

  test("setting a relationship property: MATCH (a)-[r]->(b) SET r.prop = 42 RETURN r") {
    val query = buildSinglePlannerQuery("MATCH (a)-[r]->(b) SET r.prop = 42 RETURN r")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("r" -> varFor("r")),
      isTerminating = true
    ))

    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, List(), SimplePatternLength)
    ))
    query.queryGraph.mutatingPatterns should equal(List(
      SetRelationshipPropertyPattern(varFor("r"), PropertyKeyName("prop")(pos), literalInt(42))
    ))
  }

  test("removing a relationship property should look like setting a property to null") {
    val query = buildSinglePlannerQuery("MATCH (a)-[r]->(b) REMOVE r.prop RETURN r")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("r" -> varFor("r")),
      isTerminating = true
    ))

    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, List(), SimplePatternLength)
    ))
    query.queryGraph.mutatingPatterns should equal(List(
      SetRelationshipPropertyPattern(varFor("r"), PropertyKeyName("prop")(pos), nullLiteral)
    ))
  }

  test("Query with single CREATE clause") {
    val query = buildSinglePlannerQuery("CREATE (a), (b), (a)-[r:X]->(b) RETURN a, r, b")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("a" -> varFor("a"), "r" -> varFor("r"), "b" -> varFor("b")),
      isTerminating = true
    ))

    query.queryGraph.mutatingPatterns should equal(Seq(
      CreatePattern(
        nodes("a", "b") ++ relationship("r", "a", "X", "b")
      )
    ))

    query.queryGraph.readOnly should be(false)
  }

  test("Read write and read again") {
    val query = buildSinglePlannerQuery("MATCH (n) CREATE (m) WITH * MATCH (o) RETURN *")
    query.horizon should equal(RegularQueryProjection(
      projections = Map("n" -> varFor("n"), "m" -> varFor("m"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
    query.queryGraph.mutatingPatterns should equal(Seq(CreatePattern(nodes("m"))))

    val next = query.tail.get

    next.queryGraph.patternNodes should equal(Set("o"))
    next.queryGraph.readOnly should be(true)
  }

  test("Unwind, read write and read again") {
    val query = buildSinglePlannerQuery("UNWIND [1] as i MATCH (n) CREATE (m) WITH * MATCH (o) RETURN *")
    query.horizon should equal(UnwindProjection("i", listOfInt(1)))
    query.queryGraph.isEmpty shouldBe true

    val second = query.tail.get

    second.queryGraph.patternNodes should equal(Set("n"))
    second.queryGraph.mutatingPatterns should equal(IndexedSeq(CreatePattern(nodes("m"))))

    val third = second.tail.get

    third.queryGraph.patternNodes should equal(Set("o"))
    third.queryGraph.readOnly should be(true)
  }

  test("Foreach with create node") {
    val query = buildSinglePlannerQuery("FOREACH (i in [1] | CREATE (a))")
    query.queryGraph.isEmpty shouldBe true
    val second = query.tail.get
    second.queryGraph.mutatingPatterns should equal(
      Seq(ForeachPattern(
        varFor("i"),
        listOfInt(1),
        RegularSinglePlannerQuery(
          QueryGraph(
            Set.empty,
            Set.empty,
            Set.empty,
            Set.empty,
            Selections(Set.empty),
            Vector.empty,
            Set.empty,
            Set.empty,
            IndexedSeq(CreatePattern(nodes("a")))
          ),
          InterestingOrder.empty,
          PassthroughAllHorizon(),
          None
        )
      ))
    )
  }

  test("FOREACH with MERGE uses right arguments") {
    val query = buildSinglePlannerQuery(
      """
        |WITH 5 AS x, [1, 2] AS list 
        |MATCH (b) 
        |FOREACH (i IN list | 
        |  MERGE (a {prop: x + i})-[r:R]->(b)
        |)""".stripMargin
    )
    val foreach = query.allPlannerQueries(2).queryGraph.mutatingPatterns.head.asInstanceOf[ForeachPattern]
    foreach.innerUpdates.allPlannerQueries.map(_.queryGraph.argumentIds) shouldEqual Seq(
      Set("x", "b", "i"),
      Set("x", "b", "i"),
      Set("x", "b", "i", "a", "r")
    )
  }

  test("FOREACH uses right arguments in multi-part inner-update-query") {
    val query = buildSinglePlannerQuery(
      """
        |WITH 5 AS x, 6 AS y, [1, 2] AS list 
        |MATCH (b), (c), (d)
        |FOREACH (i IN list | 
        |  MERGE (a {prop: x + i})-[r1:R]->(b)
        |  MERGE (a)-[r2:R {prop: y + i}]->(c)
        |)""".stripMargin
    )
    val foreach = query.allPlannerQueries(2).queryGraph.mutatingPatterns.head.asInstanceOf[ForeachPattern]
    foreach.innerUpdates.allPlannerQueries.map(_.queryGraph.argumentIds) shouldEqual Seq(
      Set("x", "b", "i", "y", "c"),
      Set("x", "b", "i", "y", "c"),
      Set("x", "b", "i", "y", "c", "a", "r1"),
      Set("x", "b", "i", "y", "c", "a", "r1"),
      Set("x", "b", "i", "y", "c", "a", "r1", "r2")
    )
  }

  test("nested FOREACH uses right arguments") {
    val query = buildSinglePlannerQuery(
      """
        |WITH 5 AS x, 6 AS y, [1, 2] AS list, [3, 4] AS list2, [5, 6] AS list3
        |MATCH (b), (c), (d)
        |FOREACH (i IN list | 
        |  FOREACH (j IN list2 |
        |     SET b.prop = c.prop + i + j
        |  )
        |)""".stripMargin
    )
    val outerForeach = query.allPlannerQueries(2).queryGraph.mutatingPatterns.head.asInstanceOf[ForeachPattern]
    outerForeach.innerUpdates.allPlannerQueries.map(_.queryGraph.argumentIds) shouldEqual Seq(
      Set("b", "c", "i", "list2"),
      Set("b", "c", "i", "list2"),
      Set("b", "c", "i", "list2")
    )
    val innerForeach =
      outerForeach.innerUpdates.allPlannerQueries(1).queryGraph.mutatingPatterns.head.asInstanceOf[ForeachPattern]
    innerForeach.innerUpdates.allPlannerQueries.map(_.queryGraph.argumentIds) shouldEqual Seq(
      Set("b", "c", "i", "j")
    )
  }

  test("correlated subquery with create node") {
    val query = buildSinglePlannerQuery("MATCH (n) CALL { WITH n CREATE (m) RETURN m } RETURN n, m")
    query.readOnly shouldBe false withClue "(query should not be readonly)"
    query.horizon shouldEqual CallSubqueryHorizon(
      correlated = true,
      yielding = true,
      inTransactionsParameters = None,
      callSubquery = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(
          argumentIds = Set("n"),
          mutatingPatterns = IndexedSeq(
            CreatePattern(nodes("m"))
          )
        ),
        horizon = RegularQueryProjection(
          projections = Map("m" -> varFor("m"))
        )
      )
    )
  }

  test("correlated subquery with create node without return") {
    val query = buildSinglePlannerQuery("MATCH (n) CALL { WITH n CREATE (m) } RETURN n")
    query.readOnly shouldBe false withClue "(query should not be readonly)"
    query.horizon shouldEqual CallSubqueryHorizon(
      correlated = true,
      yielding = false,
      inTransactionsParameters = None,
      callSubquery = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(
          argumentIds = Set("n"),
          mutatingPatterns = IndexedSeq(
            CreatePattern(nodes("m"))
          )
        )
      )
    )
  }

  test("Should create a relationship outgoing from a strict interior node of a shortest path pattern") {
    val query = buildSinglePlannerQuery(
      """
        |MATCH ANY SHORTEST (u) ((a)-[r]->(b))+ (v) ((c)-[r2]->(d))+ (w)
        |CREATE (v)-[r3:R]->(x)
        |RETURN x
        |""".stripMargin
    )
    val qpp1: QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"a", v"u"),
        rightBinding = NodeBinding(v"b", v"v"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r",
          boundaryNodes = (v"a", v"b"),
          dir = SemanticDirection.OUTGOING,
          types = Nil,
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, Unlimited),
        nodeVariableGroupings = Set("a", "b").map(name => variableGrouping(varFor(name), varFor(name))),
        relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
      )
    val qpp2: QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"c", v"v"),
        rightBinding = NodeBinding(v"d", v"w"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r2",
          boundaryNodes = (v"c", v"d"),
          dir = SemanticDirection.OUTGOING,
          types = Nil,
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, Unlimited),
        nodeVariableGroupings = Set("c", "d").map(name => variableGrouping(varFor(name), varFor(name))),
        relationshipVariableGroupings = Set(variableGrouping(v"r2", v"r2"))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp1, qpp2)),
        selections = Selections.from(Seq(
          unique(varFor("r")),
          unique(varFor("r2")),
          disjoint(varFor("r"), varFor("r2"))
        )),
        selector = SelectivePathPattern.Selector.Shortest(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addSelectivePathPattern(shortestPathPattern)
        .addMutatingPatterns(CreatePattern(Seq(
          CreateNode(varFor("x"), Set.empty, None),
          CreateRelationship(varFor("r3"), varFor("v"), relTypeName("R"), varFor("x"), OUTGOING, None)
        )))

    val projection = RegularQueryProjection(projections = Map("x" -> varFor("x")), isTerminating = true)

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  test("should use correct arguments in MERGE") {
    val merges = Table(
      "Merge query" -> "Expected dependencies",
      "MERGE (a) ON MATCH SET a.p = five" -> Set("five"),
      "MERGE (a) ON CREATE SET a.p = five" -> Set("five"),
      "MERGE (a {p: five})" -> Set("five"),
      "MERGE (prev)-[r:R]->(a) ON MATCH SET a.p = five" -> Set("prev", "five"),
      "MERGE (prev)-[r:R]->(a) ON CREATE SET a.p = five" -> Set("prev", "five"),
      "MERGE (prev)-[r:R]->(a) ON MATCH SET prev.p = five" -> Set("prev", "five"),
      "MERGE (prev)-[r:R]->(a) ON CREATE SET prev.p = five" -> Set("prev", "five"),
      "MERGE (prev)-[r:R]->(a {p: five})" -> Set("prev", "five"),
      "MERGE (prev)-[r:R {p: five}]->(a)" -> Set("prev", "five"),
      "MERGE (prev)-[r:R {p: a.p}]->(a)" -> Set("prev"),
      "MERGE (prev)-[r:R {p: prev.p}]->(a)" -> Set("prev"),
      "MERGE (prev)-[r:R]->(a)" -> Set("prev"),
      "MERGE (prev)-[r:R]->(a)-[r2:R]->(b)" -> Set("prev"),
      "MERGE (prev)-[r:R]->(a)-[r2:R {p:r.p}]->(b)" -> Set("prev")
    )

    forAll(merges) {
      case (merge, expectedDeps) =>
        val query = buildSinglePlannerQuery(
          s"""
             |MATCH (prev)
             |WITH prev, 5 AS five
             |$merge
             |""".stripMargin
        )
        // MERGE is always isolated in an own query graph, which is why we find the MERGE
        // in the _second_ last query graph.
        query.allPlannerQueries(query.allPlannerQueries.length - 2)
          .queryGraph
          .mergeQueryGraph
          .map(_.argumentIds) shouldEqual Some(expectedDeps)
    }
  }

  private def nodes(names: String*) = {
    names.map(varFor).map(name => CreateNode(name, Set.empty, None))
  }

  private def relationship(name: String, startNode: String, relType: String, endNode: String) = {
    List(CreateRelationship(
      varFor(name),
      varFor(startNode),
      RelTypeName(relType)(pos),
      varFor(endNode),
      OUTGOING,
      None
    ))
  }
}
