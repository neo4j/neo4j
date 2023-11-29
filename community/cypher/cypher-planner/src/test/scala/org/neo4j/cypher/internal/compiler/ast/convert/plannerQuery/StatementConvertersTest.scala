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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.builder.Parser
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.GpmShortestPath,
    SemanticFeature.MatchModes
  )
  private val patternRel = PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
  private val nProp = prop("n", "prop")

  test("CALL around single query") {
    val query = buildSinglePlannerQuery("CALL { RETURN 1 as x } RETURN 2 as y")
    query.horizon should equal(CallSubqueryHorizon(
      RegularSinglePlannerQuery(
        horizon = RegularQueryProjection(Map("x" -> literalInt(1)))
      ),
      correlated = false,
      yielding = true,
      inTransactionsParameters = None
    ))

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(2)), isTerminating = true))
  }

  test("CALL around single correlated query") {
    val query = buildSinglePlannerQuery("WITH 1 AS x CALL { WITH x RETURN x as y } RETURN y")

    query.horizon should equal(RegularQueryProjection(Map("x" -> literalInt(1))))

    query.tail should not be empty
    val subQuery = query.tail.get

    subQuery.horizon should equal(
      CallSubqueryHorizon(
        correlated = true,
        yielding = true,
        inTransactionsParameters = None,
        callSubquery = RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("x")),
          horizon = RegularQueryProjection(Map("y" -> varFor("x")))
        )
      )
    )

    subQuery.tail should not be empty
    val nextQuery = subQuery.tail.get

    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> varFor("y")), isTerminating = true))
  }

  test("CALL around single query - using returned var in outer query") {
    val query = buildSinglePlannerQuery("CALL { RETURN 1 as x } RETURN x")
    query.horizon should equal(CallSubqueryHorizon(
      RegularSinglePlannerQuery(
        horizon = RegularQueryProjection(Map("x" -> literalInt(1)))
      ),
      correlated = false,
      yielding = true,
      inTransactionsParameters = None
    ))

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x")), isTerminating = true))
  }

  test("CALL around union query") {
    val query = buildSinglePlannerQuery("CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN 3 as y")
    query.horizon should equal(CallSubqueryHorizon(
      UnionQuery(
        RegularSinglePlannerQuery(
          horizon = RegularQueryProjection(Map("x" -> literalInt(1)))
        ),
        RegularSinglePlannerQuery(
          horizon = RegularQueryProjection(Map("x" -> literalInt(2)))
        ),
        distinct = true,
        List(UnionMapping(varFor("x"), varFor("x"), varFor("x")))
      ),
      correlated = false,
      yielding = true,
      inTransactionsParameters = None
    ))

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(3)), isTerminating = true))
  }

  test("CALL around correlated union query") {
    val query =
      buildSinglePlannerQuery("WITH 1 AS x, 2 AS n CALL { WITH x RETURN x as y UNION RETURN 2 as y } RETURN y")

    query.horizon should equal(RegularQueryProjection(Map("x" -> literalInt(1), "n" -> literalInt(2))))

    query.tail should not be empty

    val subquery = query.tail.get
    subquery.horizon should equal(CallSubqueryHorizon(
      correlated = true,
      yielding = true,
      inTransactionsParameters = None,
      callSubquery = UnionQuery(
        RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("x")),
          horizon = RegularQueryProjection(Map("y" -> varFor("x")))
        ),
        RegularSinglePlannerQuery(
          horizon = RegularQueryProjection(Map("y" -> literalInt(2)))
        ),
        distinct = true,
        List(UnionMapping(varFor("y"), varFor("y"), varFor("y")))
      )
    ))

    subquery.tail should not be empty

    val nextQuery = subquery.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> varFor("y")), isTerminating = true))
  }

  test("CALL around union query - using returned var in outer query") {
    val query = buildSinglePlannerQuery("CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x")
    query.horizon should equal(CallSubqueryHorizon(
      UnionQuery(
        RegularSinglePlannerQuery(
          horizon = RegularQueryProjection(Map("x" -> literalInt(1)))
        ),
        RegularSinglePlannerQuery(
          horizon = RegularQueryProjection(Map("x" -> literalInt(2)))
        ),
        distinct = true,
        List(UnionMapping(varFor("x"), varFor("x"), varFor("x")))
      ),
      correlated = false,
      yielding = true,
      inTransactionsParameters = None
    ))

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x")), isTerminating = true))
  }

  test("CALL unit subquery in transactions") {
    val query = buildSinglePlannerQuery("CALL { CREATE (x) } IN TRANSACTIONS RETURN 2 as y")
    query.horizon shouldEqual CallSubqueryHorizon(
      RegularSinglePlannerQuery(queryGraph =
        QueryGraph.empty.addMutatingPatterns(CreatePattern(Seq(createNodeIr("x"))))
      ),
      correlated = false,
      yielding = false,
      inTransactionsParameters = Some(inTransactionsParameters(None, None, None))
    )

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(2)), isTerminating = true))
  }

  test("CALL correlated unit subquery in transactions") {
    val query = buildSinglePlannerQuery("WITH 1 AS n CALL { WITH n CREATE (x) } IN TRANSACTIONS RETURN 2 as y")

    query.horizon shouldEqual RegularQueryProjection(Map("n" -> literalInt(1)))

    query.tail should not be empty
    val subQuery = query.tail.get

    subQuery.horizon shouldEqual CallSubqueryHorizon(
      RegularSinglePlannerQuery(queryGraph =
        QueryGraph.empty
          .addMutatingPatterns(CreatePattern(Seq(createNodeIr("x"))))
          .addArgumentId("n")
      ),
      correlated = true,
      yielding = false,
      inTransactionsParameters = Some(inTransactionsParameters(None, None, None))
    )

    subQuery.tail should not be empty

    val nextQuery = subQuery.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(2)), isTerminating = true))
  }

  test("CALL subquery in transactions") {
    val query = buildSinglePlannerQuery("CALL { CREATE (x) RETURN x } IN TRANSACTIONS RETURN 2 as y")
    query.horizon shouldEqual CallSubqueryHorizon(
      RegularSinglePlannerQuery(
        queryGraph = QueryGraph.empty.addMutatingPatterns(CreatePattern(Seq(createNodeIr("x")))),
        horizon = RegularQueryProjection(Map("x" -> varFor("x")))
      ),
      correlated = false,
      yielding = true,
      inTransactionsParameters = Some(inTransactionsParameters(None, None, None))
    )

    query.tail should not be empty

    val nextQuery = query.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(2)), isTerminating = true))
  }

  test("CALL correlated subquery in transactions") {
    val query = buildSinglePlannerQuery("WITH 1 AS n CALL { WITH n CREATE (x) RETURN x } IN TRANSACTIONS RETURN 2 as y")

    query.horizon shouldEqual RegularQueryProjection(Map("n" -> literalInt(1)))

    query.tail should not be empty
    val subQuery = query.tail.get

    subQuery.horizon shouldEqual CallSubqueryHorizon(
      RegularSinglePlannerQuery(
        horizon = RegularQueryProjection(Map("x" -> varFor("x"))),
        queryGraph = QueryGraph.empty
          .addMutatingPatterns(CreatePattern(Seq(createNodeIr("x"))))
          .addArgumentId("n")
      ),
      correlated = true,
      yielding = true,
      inTransactionsParameters = Some(inTransactionsParameters(None, None, None))
    )

    subQuery.tail should not be empty

    val nextQuery = subQuery.tail.get
    nextQuery.horizon should equal(RegularQueryProjection(Map("y" -> literalInt(2)), isTerminating = true))
  }

  test("RETURN 42") {
    val query = buildSinglePlannerQuery("RETURN 42")
    query.horizon should equal(RegularQueryProjection(Map("42" -> literalInt(42)), isTerminating = true))
  }

  test("RETURN 42, 'foo'") {
    val query = buildSinglePlannerQuery("RETURN 42, 'foo'")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "42" -> literalInt(42),
        "'foo'" -> literalString("foo")
      ),
      isTerminating = true
    ))
  }

  test("match (n) return n") {
    val query = buildSinglePlannerQuery("match (n) return n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:A:B RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) WHERE n:A:B RETURN n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A")),
      Predicate(Set("n"), hasLabels("n", "B"))
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) where n:X OR n:Y return n") {
    val query = buildSinglePlannerQuery("match (n) where n:X OR n:Y return n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(
        Set("n"),
        ors(
          hasLabels("n", "X"),
          hasLabels("n", "Y")
        )
      )
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) WHERE n:X OR (n:A AND n:B) RETURN n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(
        Set("n"),
        ors(
          hasLabels("n", "X"),
          hasLabels("n", "B")
        )
      ),
      Predicate(
        Set("n"),
        ors(
          hasLabels("n", "X"),
          hasLabels("n", "A")
        )
      )
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE id(n) = 42 RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) WHERE id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(
        Set("n"),
        in(
          id(varFor("n")),
          listOfInt(42)
        )
      )
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE id(n) IN [42, 43] RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) WHERE id(n) IN [42, 43] RETURN n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(
        Set("n"),
        in(
          id(varFor("n")),
          listOfInt(42, 43)
        )
      )
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n) WHERE n:A AND id(n) = 42 RETURN n")
    query.horizon should equal(RegularQueryProjection(
      Map(
        "n" -> varFor("n")
      ),
      isTerminating = true
    ))

    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A")),
      Predicate(
        Set("n"),
        in(
          id(varFor("n")),
          listOfInt(42)
        )
      )
    )))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match p = (a) return p") {
    val query = buildSinglePlannerQuery("match p = (a) return p")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set[String]("a"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(
      Map[String, Expression](
        "p" -> PathExpression(NodePathStep(varFor("a"), NilPathStep()(pos))(pos)) _
      ),
      isTerminating = true
    ))
  }

  test("match p = (a)-[r]->(b) return a,r") {
    val query = buildSinglePlannerQuery("match p = (a)-[r]->(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(patternRel))
    query.queryGraph.patternNodes should equal(Set[String]("a", "b"))
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(c) return a,r,b, c") {
    val query = buildSinglePlannerQuery("match (a)-[r]->(b)-[r2]->(c) return a,r,b")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    query.queryGraph.selections should equal(Selections.from(differentRelationships("r2", "r")))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r"),
        "b" -> varFor("b")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r]->(b)-[r2]->(a) return a,r") {
    val query = buildSinglePlannerQuery("match (a)-[r]->(b)-[r2]->(a) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength),
      PatternRelationship(v"r2", (v"b", v"a"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections.from(differentRelationships("r2", "r")))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)<-[r]-(b)-[r2]-(c) return a,r") {
    val query = buildSinglePlannerQuery("match (a)<-[r]-(b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(v"r2", (v"b", v"c"), BOTH, Seq.empty, SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    query.queryGraph.selections should equal(Selections.from(differentRelationships("r2", "r")))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r") {
    val query = buildSinglePlannerQuery("match (a)<-[r]-(b), (b)-[r2]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), INCOMING, Seq.empty, SimplePatternLength),
      PatternRelationship(v"r2", (v"b", v"c"), BOTH, Seq.empty, SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    val predicate = Predicate(Set("r", "r2"), differentRelationships("r", "r2"))
    query.queryGraph.selections.predicates should equal(Set(predicate))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a), (n)-[r:Type]-(c) where b:A return a,r") {
    val query = buildSinglePlannerQuery("match (a), (n)-[r:Type]-(c) where n:A return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"n", v"c"), BOTH, Seq(relTypeName("Type")), SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "n", "c"))
    query.queryGraph.selections should equal(Selections(Set(
      Predicate(Set("n"), hasLabels("n", "A"))
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r:Type|Foo]-(b) return a,r") {
    val query = buildSinglePlannerQuery("match (a)-[r:Type|Foo]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq(relTypeName("Type"), relTypeName("Foo")), SimplePatternLength)
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections())
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r:Type*]-(b) return a,r") {
    val query = buildSinglePlannerQuery("match (a)-[r:Type*]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq(relTypeName("Type")), VarPatternLength(1, None))
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections.from(Seq(
      unique(varFor("r")),
      varLengthLowerLimitPredicate("r", 1)
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c") {
    val query = buildSinglePlannerQuery("match (a)-[r1:CONTAINS*0..1]->(b)-[r2:FRIEND*0..1]->(c) return a,b,c")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r1", (v"a", v"b"), OUTGOING, Seq(relTypeName("CONTAINS")), VarPatternLength(0, Some(1))),
      PatternRelationship(v"r2", (v"b", v"c"), OUTGOING, Seq(relTypeName("FRIEND")), VarPatternLength(0, Some(1)))
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))
    query.queryGraph.selections should equal(Selections.from(Seq(
      unique(varFor("r1")),
      unique(varFor("r2")),
      varLengthLowerLimitPredicate("r1", 0),
      varLengthLowerLimitPredicate("r2", 0),
      varLengthUpperLimitPredicate("r1", 1),
      varLengthUpperLimitPredicate("r2", 1)
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "b" -> varFor("b"),
        "c" -> varFor("c")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r:Type*3..]-(b) return a,r") {
    val query = buildSinglePlannerQuery("match (a)-[r:Type*3..]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq(relTypeName("Type")), VarPatternLength(3, None))
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections.from(Seq(
      unique(varFor("r")),
      varLengthLowerLimitPredicate("r", 3)
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)-[r:Type*5]-(b) return a,r") {
    val query = buildSinglePlannerQuery("match (a)-[r:Type*5]-(b) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq(relTypeName("Type")), VarPatternLength.fixed(5))
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.selections should equal(Selections.from(Seq(
      unique(varFor("r")),
      varLengthLowerLimitPredicate("r", 5),
      varLengthUpperLimitPredicate("r", 5)
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("match (a)<-[r*]-(b)-[r2*]-(c) return a,r") {
    val query = buildSinglePlannerQuery("match (a)<-[r*]-(b)-[r2*]-(c) return a,r")
    query.queryGraph.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), INCOMING, Seq.empty, VarPatternLength(1, None)),
      PatternRelationship(v"r2", (v"b", v"c"), BOTH, Seq.empty, VarPatternLength(1, None))
    ))
    query.queryGraph.patternNodes should equal(Set("a", "b", "c"))

    query.queryGraph.selections should equal(Selections.from(Seq(
      unique(varFor("r")),
      unique(varFor("r2")),
      disjoint(varFor("r2"), varFor("r")),
      varLengthLowerLimitPredicate("r", 1),
      varLengthLowerLimitPredicate("r2", 1)
    )))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
  }

  test("optional match (a) return a") {
    val query = buildSinglePlannerQuery("optional match (a) return a")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a")
      ),
      isTerminating = true
    ))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set())
    optMatchQG.patternNodes should equal(Set("a"))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("optional match (a)-[r]->(b) return a,b,r") {
    val query = buildSinglePlannerQuery("optional match (a)-[r]->(b) return a,b,r")
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.patternNodes should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "b" -> varFor("b"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))

    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.patternNodes should equal(Set("a", "b"))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set())
  }

  test("match (a) optional match (a)-[r]->(b) return a,b,r") {
    val query = buildSinglePlannerQuery("match (a) optional match (a)-[r]->(b) return a,b,r")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.patternRelationships should equal(Set())
    query.queryGraph.selections should equal(Selections(Set.empty))
    query.horizon should equal(RegularQueryProjection(
      Map(
        "a" -> varFor("a"),
        "b" -> varFor("b"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    ))
    query.queryGraph.optionalMatches.size should equal(1)
    query.queryGraph.argumentIds should equal(Set())

    val optMatchQG = query.queryGraph.optionalMatches.head
    optMatchQG.patternNodes should equal(Set("a", "b"))
    optMatchQG.patternRelationships should equal(Set(
      PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, SimplePatternLength)
    ))
    optMatchQG.selections should equal(Selections(Set.empty))
    optMatchQG.optionalMatches should be(empty)
    optMatchQG.argumentIds should equal(Set("a"))
  }

  test("match (a) where (a)-->() return a") {
    // Given
    val query = buildSinglePlannerQuery("match (a) where (a)-->() return a")

    // Then inner pattern query graph
    val relName = "anon_0"
    val nodeName = "anon_1"
    val existsVariable = varFor("anon_2")
    val exp = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set("a"),
          patternNodes = Set("a", nodeName),
          patternRelationships =
            Set(PatternRelationship(varFor(relName), (v"a", varFor(nodeName)), OUTGOING, Seq(), SimplePatternLength))
        )
      ),
      existsVariable,
      s"EXISTS { MATCH (a)-[`$relName`]->(`$nodeName`) }"
    )(pos, None, None)
    val predicate = Predicate(Set("a"), exp)
    val selections = Selections(Set(predicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("MATCH (a) WITH 1 AS b RETURN b") {
    val query = buildSinglePlannerQuery("MATCH (a) WITH 1 AS b RETURN b")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(RegularQueryProjection(Map("b" -> literalInt(1))))
    query.tail should equal(Some(RegularSinglePlannerQuery(
      QueryGraph(argumentIds = Set("b")),
      InterestingOrder.empty,
      RegularQueryProjection(Map("b" -> varFor("b")), isTerminating = true)
    )))
  }

  test("WITH 1 AS b RETURN b") {
    val query = buildSinglePlannerQuery("WITH 1 AS b RETURN b")

    query.horizon should equal(RegularQueryProjection(Map("b" -> literalInt(1))))
    query.tail should equal(Some(RegularSinglePlannerQuery(
      QueryGraph(argumentIds = Set("b")),
      InterestingOrder.empty,
      RegularQueryProjection(Map("b" -> varFor("b")), isTerminating = true)
    )))
  }

  test("MATCH (a) WITH a WHERE TRUE RETURN a") {
    val result = buildSinglePlannerQuery("MATCH (a) WITH a WHERE TRUE RETURN a")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("a"), selections = Selections(Set(Predicate(Set.empty, trueLiteral)))),
      horizon = RegularQueryProjection(projections = Map("a" -> varFor("a")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("match (a) where a.prop = 42 OR (a)-->() return a") {
    // Given
    val query = buildSinglePlannerQuery("match (a) where a.prop = 42 OR (a)-->() return a")

    // Then inner pattern query graph
    val relName = "anon_0"
    val nodeName = "anon_1"
    val existsVariable = varFor("anon_2")

    val exp1 = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set("a"),
          patternNodes = Set("a", nodeName),
          patternRelationships =
            Set(PatternRelationship(varFor(relName), (v"a", varFor(nodeName)), OUTGOING, Seq(), SimplePatternLength))
        )
      ),
      existsVariable,
      s"EXISTS { MATCH (a)-[`$relName`]->(`$nodeName`) }"
    )(pos, None, None)

    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp2))
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (a) where (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildSinglePlannerQuery("match (a) where (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "anon_0"
    val nodeName = "anon_1"
    val existsVariable = varFor("anon_2")

    val exp1 = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set("a"),
          patternNodes = Set("a", nodeName),
          patternRelationships =
            Set(PatternRelationship(varFor(relName), (v"a", varFor(nodeName)), OUTGOING, Seq(), SimplePatternLength))
        )
      ),
      existsVariable,
      s"EXISTS { MATCH (a)-[`$relName`]->(`$nodeName`) }"
    )(pos, None, None)

    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp2))
    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a") {
    // Given
    val query = buildSinglePlannerQuery("match (a) where a.prop2 = 21 OR (a)-->() OR a.prop = 42 return a")

    // Then inner pattern query graph
    val relName = "anon_0"
    val nodeName = "anon_1"
    val existsVariable = varFor("anon_2")

    val exp1 = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set("a"),
          patternNodes = Set("a", nodeName),
          patternRelationships =
            Set(PatternRelationship(varFor(relName), (v"a", varFor(nodeName)), OUTGOING, Seq(), SimplePatternLength))
        )
      ),
      existsVariable,
      s"EXISTS { MATCH (a)-[`$relName`]->(`$nodeName`) }"
    )(pos, None, None)

    val exp2 = in(prop("a", "prop"), listOfInt(42))
    val exp3 = in(prop("a", "prop2"), listOfInt(21))
    val orPredicate = Predicate(Set("a"), ors(exp1, exp2, exp3))

    val selections = Selections(Set(orPredicate))

    query.queryGraph.selections should equal(selections)
    query.queryGraph.patternNodes should equal(Set("a"))
  }

  test("match (n) return n limit 10") {
    // Given
    val query = buildSinglePlannerQuery("match (n) return n limit 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set("n"))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n" -> varFor("n")),
        QueryPagination(
          skip = None,
          limit = Some(literalInt(10))
        ),
        isTerminating = true
      )
    )
  }

  test("match (n) return n skip 10") {
    // Given
    val query = buildSinglePlannerQuery("match (n) return n skip 10")

    // Then inner pattern query graph
    query.queryGraph.selections should equal(Selections())
    query.queryGraph.patternNodes should equal(Set("n"))
    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("n" -> varFor("n")),
        QueryPagination(
          skip = Some(literalInt(10)),
          limit = None
        ),
        isTerminating = true
      )
    )
  }

  test("match (a) with * return a") {
    val query = buildSinglePlannerQuery("match (a) with * return a")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(RegularQueryProjection(
      Map[String, Expression]("a" -> varFor("a")),
      isTerminating = true
    ))
    query.tail should equal(None)
  }

  test("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b") {
    val query = buildSinglePlannerQuery("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN a, b")
    query.queryGraph should equal(
      QueryGraph
        .empty
        .addPatternNodes("a")
    )

    query.horizon should equal(
      RegularQueryProjection(
        projections = Map("a" -> varFor("a")),
        QueryPagination(
          skip = None,
          limit = Some(literalInt(1))
        )
      )
    )

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
    val query = buildSinglePlannerQuery("optional match (a:Foo) with a match (a)-[r]->(b) return a")

    query.queryGraph should equal(
      QueryGraph
        .empty
        .addOptionalMatch(
          QueryGraph
            .empty
            .addPatternNodes("a")
            .addSelections(Selections(Set(Predicate(Set("a"), hasLabels("a", "Foo")))))
        )
    )
    query.horizon should equal(RegularQueryProjection(Map("a" -> varFor("a"))))
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
    val query = buildSinglePlannerQuery(
      "MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b"
    )
    query.tail should not be empty
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> prop("a", "prop")),
        QueryPagination(None, Some(literalInt(1)))
      )
    )
    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set("b"))
    tailQg.queryGraph.patternRelationships should be(empty)

    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryPagination(),
        isTerminating = true
      )
    )
  }

  test("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b") {
    val query =
      buildSinglePlannerQuery("MATCH (a:Start) WITH a.prop AS property MATCH (b) WHERE id(b) = property RETURN b")
    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))

    query.horizon should equal(
      RegularQueryProjection(
        Map("property" -> prop("a", "prop"))
      )
    )

    val secondQuery = query.tail.get
    secondQuery.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )
    ))

    secondQuery.horizon should equal(
      RegularQueryProjection(
        Map("b" -> varFor("b")),
        isTerminating = true
      )
    )
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val query = buildSinglePlannerQuery(
      "MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b"
    )
    query.tail should not be empty
    query.queryGraph.selections.predicates should equal(Set(
      Predicate(Set("a"), hasLabels("a", "Start"))
    ))
    query.queryGraph.patternNodes should equal(Set("a"))
    query.horizon should equal(AggregatingQueryProjection(
      Map("property" -> prop("a", "prop")),
      Map("count" -> CountStar() _)
    ))

    val tailQg = query.tail.get
    tailQg.queryGraph.patternNodes should equal(Set("b"))
    tailQg.queryGraph.patternRelationships should be(empty)
    tailQg.queryGraph.selections.predicates should equal(Set(
      Predicate(
        Set("b", "property"),
        in(id(varFor("b")), listOf(varFor("property")))
      )
    ))

    tailQg.horizon should equal(
      RegularQueryProjection(
        projections = Map("b" -> varFor("b")),
        QueryPagination(),
        isTerminating = true
      )
    )
  }

  test("MATCH (n) RETURN count(*)") {
    val query = buildSinglePlannerQuery("MATCH (n) RETURN count(*)")

    query.horizon match {
      case AggregatingQueryProjection(
          groupingKeys,
          aggregationExpression,
          QueryPagination(limit, skip),
          where,
          _
        ) => // TODO: Update
        groupingKeys should be(empty)
        limit should be(empty)
        skip should be(empty)
        where should be(empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be(empty)
    query.queryGraph.patternRelationships should be(empty)
    query.queryGraph.patternNodes should be(Set("n"))
  }

  test("MATCH (n) RETURN n.prop, count(*)") {
    val query = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, count(*)")

    query.horizon match {
      case AggregatingQueryProjection(
          groupingKeys,
          aggregationExpression,
          QueryPagination(limit, skip),
          where,
          _
        ) => // TODO:Update
        groupingKeys should equal(Map("n.prop" -> nProp))
        limit should be(empty)
        skip should be(empty)
        where should be(empty)
        aggregationExpression should equal(Map("count(*)" -> CountStar()(pos)))

      case x =>
        fail(s"Expected AggregationProjection, got $x")
    }

    query.queryGraph.selections.predicates should be(empty)
    query.queryGraph.patternRelationships should be(empty)
    query.queryGraph.patternNodes should be(Set("n"))
  }

  test("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n") {
    val query = buildSinglePlannerQuery("MATCH (n:Awesome {prop: 42}) USING INDEX n:Awesome(prop) RETURN n")

    query.queryGraph.hints should equal(Set[Hint](UsingIndexHint(
      varFor("n"),
      labelOrRelTypeName("Awesome"),
      Seq(PropertyKeyName("prop")(pos))
    ) _))
  }

  test("MATCH shortestPath((a)-[r]->(b)) RETURN r") {
    val query = buildSinglePlannerQuery("MATCH shortestPath((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestRelationshipPatterns should equal(Set(
      ShortestRelationshipPattern(
        Some(v"anon_0"),
        PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, VarPatternLength(1, Some(1))),
        single = true
      )(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH allShortestPaths((a)-[r]->(b)) RETURN r") {
    val query = buildSinglePlannerQuery("MATCH allShortestPaths((a)-[r]->(b)) RETURN r")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestRelationshipPatterns should equal(Set(
      ShortestRelationshipPattern(
        Some(v"anon_0"),
        PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, VarPatternLength(1, Some(1))),
        single = false
      )(null)
    ))
    query.tail should be(empty)
  }

  test("MATCH p = shortestPath((a)-[r]->(b)) RETURN p") {
    val query = buildSinglePlannerQuery("MATCH p = shortestPath((a)-[r]->(b)) RETURN p")

    query.queryGraph.patternNodes should equal(Set("a", "b"))
    query.queryGraph.shortestRelationshipPatterns should equal(Set(
      ShortestRelationshipPattern(
        Some(v"p"),
        PatternRelationship(v"r", (v"a", v"b"), OUTGOING, Seq.empty, VarPatternLength(1, Some(1))),
        single = true
      )(null)
    ))
    query.tail should be(empty)
  }

  test("match (n) return distinct n") {
    val query = buildSinglePlannerQuery("match (n) return distinct n")
    query.horizon should equal(DistinctQueryProjection(
      groupingExpressions = Map("n" -> varFor("n")),
      isTerminating = true
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) with distinct n.prop as x return x") {
    val query = buildSinglePlannerQuery("match (n) with distinct n.prop as x return x")
    query.horizon should equal(DistinctQueryProjection(
      groupingExpressions = Map("x" -> nProp)
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("match (n) with distinct * return n") {
    val query = buildSinglePlannerQuery("match (n) with distinct * return n")
    query.horizon should equal(DistinctQueryProjection(
      groupingExpressions = Map("n" -> varFor("n"))
    ))

    query.queryGraph.patternNodes should equal(Set("n"))
  }

  test("WITH DISTINCT 1 as b RETURN b") {
    val query = buildSinglePlannerQuery("WITH DISTINCT 1 as b RETURN b")

    query.horizon should equal(DistinctQueryProjection(
      groupingExpressions = Map("b" -> literalInt(1))
    ))

    query.tail should not be empty
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner") {
    val result =
      buildSinglePlannerQuery("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN owner")

    val relName = "anon_0"
    val nodeName = "anon_1"
    val existsVariable = varFor("anon_2")

    // (owner)-[relName]-(nodeName)
    val subqueryExpression = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set("owner"),
          patternNodes = Set("owner", nodeName),
          patternRelationships =
            Set(PatternRelationship(varFor(relName), (v"owner", varFor(nodeName)), BOTH, Seq(), SimplePatternLength))
        )
      ),
      existsVariable,
      s"EXISTS { MATCH (owner)-[`$relName`]-(`$nodeName`) }"
    )(pos, Some(Set(varFor(nodeName), varFor(relName))), Some(Set(varFor("owner"))))

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("owner")),
      horizon = AggregatingQueryProjection(
        groupingExpressions = Map("owner" -> varFor("owner")),
        aggregationExpressions = Map("collected" -> CountStar()(pos)),
        selections = Selections(Set(Predicate(Set("owner"), subqueryExpression)))
      ),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("collected", "owner")),
        horizon = RegularQueryProjection(projections = Map("owner" -> varFor("owner")), isTerminating = true)
      ))
    )

    result should equal(expectation)
  }

  test("UNWIND [1,2,3] AS x RETURN x") {
    val query = buildSinglePlannerQuery("UNWIND [1,2,3] AS x RETURN x")

    query.horizon should equal(UnwindProjection(
      "x",
      listOfInt(1, 2, 3)
    ))

    val tail = query.tail.get

    tail.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x")), isTerminating = true))
  }

  test("CALL foo() YIELD all RETURN all") {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "foo"),
      inputSignature = IndexedSeq.empty,
      deprecationInfo = None,
      outputSignature = Some(IndexedSeq(FieldSignature("all", CTInteger))),
      accessMode = ProcedureReadOnlyAccess,
      id = 42
    )
    val query = buildSinglePlannerQuery("CALL foo() YIELD all RETURN all", Some(_ => signature))

    query.horizon match {
      case ProcedureCallProjection(call) =>
        call.signature should equal(signature)
      case _ =>
        fail("Built wrong query horizon")
    }
  }

  test("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y") {
    val query = buildSinglePlannerQuery("WITH [1,2,3] as xes, 2 as y UNWIND xes AS x RETURN x, y")

    query.horizon should equal(RegularQueryProjection(
      projections = Map("xes" -> listOfInt(1, 2, 3), "y" -> literalInt(2))
    ))

    val tail = query.tail.get

    tail.horizon should equal(UnwindProjection(
      "x",
      varFor("xes")
    ))

    val tailOfTail = tail.tail.get
    val graph = tailOfTail.queryGraph

    graph.argumentIds should equal(Set("xes", "x", "y"))
  }

  test("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n") {
    val query = buildSinglePlannerQuery("UNWIND [1,2,3] AS x MATCH (n) WHERE n.prop = x RETURN n")

    query.horizon should equal(UnwindProjection(
      "x",
      listOfInt(1, 2, 3)
    ))

    val tail = query.tail.get

    tail.queryGraph.patternNodes should equal(Set("n"))
    val set = Set(Predicate(Set("n", "x"), in(nProp, listOf(varFor("x")))))

    tail.queryGraph.selections.predicates should equal(set)
    tail.horizon should equal(RegularQueryProjection(Map("n" -> varFor("n")), isTerminating = true))
  }

  test("MATCH (n) UNWIND n.prop as x RETURN x") {
    val query = buildSinglePlannerQuery("MATCH (n) UNWIND n.prop as x RETURN x")

    query.queryGraph.patternNodes should equal(Set("n"))

    query.horizon should equal(UnwindProjection(
      "x",
      nProp
    ))

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(RegularQueryProjection(Map("x" -> varFor("x")), isTerminating = true))
  }

  test("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node") {
    val query = buildSinglePlannerQuery("MATCH (row) WITH collect(row) AS rows UNWIND rows AS node RETURN node")

    query.queryGraph.patternNodes should equal(Set("row"))

    query.horizon should equal(
      AggregatingQueryProjection(
        groupingExpressions = Map.empty,
        aggregationExpressions = Map("rows" -> collect(varFor("row"))),
        queryPagination = QueryPagination.empty
      )
    )

    val tail = query.tail.get
    tail.queryGraph.patternNodes should equal(Set.empty)
    tail.horizon should equal(UnwindProjection(
      "node",
      varFor("rows")
    ))

    val tailOfTail = tail.tail.get
    tailOfTail.queryGraph.patternNodes should equal(Set.empty)
    tailOfTail.horizon should equal(
      RegularQueryProjection(
        projections = Map("node" -> varFor("node")),
        isTerminating = true
      )
    )
  }

  test(
    "MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d"
  ) {
    val query = buildSinglePlannerQuery(
      "MATCH (a:A) OPTIONAL MATCH (a)-->(b:B) OPTIONAL MATCH (a)-->(c:C) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d"
    )

    query.queryGraph.patternNodes should equal(Set("a"))
    query.queryGraph.patternRelationships should equal(Set.empty)

    query.horizon should equal(RegularQueryProjection(
      projections = Map("x" -> function("coalesce", varFor("b"), varFor("c")))
    ))

    val optionalMatch1 = query.queryGraph.optionalMatches(0)
    val optionalMatch2 = query.queryGraph.optionalMatches(1)

    optionalMatch1.argumentIds should equal(Set("a"))
    optionalMatch1.patternNodes should equal(Set("a", "b"))

    optionalMatch2.argumentIds should equal(Set("a"))
    optionalMatch2.patternNodes should equal(Set("a", "c"))

    val tail = query.tail.get
    tail.queryGraph.argumentIds should equal(Set("x"))
    tail.queryGraph.patternNodes should equal(Set("x", "d"))

    tail.queryGraph.optionalMatches should be(empty)
  }

  test("OPTIONAL MATCH with dependency on shortest path") {
    val query = buildSinglePlannerQuery(
      "MATCH p=shortestPath( (a)-[r*]-(a0) ) OPTIONAL MATCH (a)--(b) WHERE b <> head(nodes(p)) RETURN p"
    )

    query.queryGraph.patternNodes should equal(Set("a", "a0"))
    query.queryGraph.patternRelationships should equal(Set.empty)

    val optionalMatch = query.queryGraph.optionalMatches(0)

    optionalMatch.argumentIds should equal(Set("a", "p"))
  }

  test("WITH-separated OPTIONAL MATCHes should be built into the same SinglePlannerQuery") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)
        |OPTIONAL MATCH (a)-[r]-(b)
        |WITH a, b
        |OPTIONAL MATCH (a)-[s]-(c)
        |WITH *
        |OPTIONAL MATCH (c)-[t]-(d)
        |WITH a, b, c, d
        |MATCH (e) // This cannot be merged with the previous SingePlannerQuery any more
        |RETURN *
        |""".stripMargin
    )

    query should equal(RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a"),
        optionalMatches = IndexedSeq(
          QueryGraph(
            argumentIds = Set("a"),
            patternNodes = Set("a", "b"),
            patternRelationships = Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength))
          ),
          QueryGraph(
            argumentIds = Set("a"),
            patternNodes = Set("a", "c"),
            patternRelationships = Set(PatternRelationship(v"s", (v"a", v"c"), BOTH, Seq.empty, SimplePatternLength))
          ),
          QueryGraph(
            argumentIds = Set("c"),
            patternNodes = Set("c", "d"),
            patternRelationships = Set(PatternRelationship(v"t", (v"c", v"d"), BOTH, Seq.empty, SimplePatternLength))
          )
        )
      ),
      horizon =
        RegularQueryProjection(Map("a" -> varFor("a"), "b" -> varFor("b"), "c" -> varFor("c"), "d" -> varFor("d"))),
      tail = Some(
        RegularSinglePlannerQuery(
          QueryGraph(
            argumentIds = Set("a", "b", "c", "d"),
            patternNodes = Set("e")
          ),
          horizon = RegularQueryProjection(
            Map(
              "a" -> varFor("a"),
              "b" -> varFor("b"),
              "c" -> varFor("c"),
              "d" -> varFor("d"),
              "e" -> varFor("e")
            ),
            isTerminating = true
          )
        )
      )
    ))
  }

  test("WITH/WHERE-separated OPTIONAL MATCHes should not be built into the same SinglePlannerQuery") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)
        |OPTIONAL MATCH (a)-[r]-(b)
        |WITH a, b WHERE a.prop > b.prop // WHERE clauses prevents the building into the same SinglePlannerQuery
        |OPTIONAL MATCH (a)-[s]-(c)
        |RETURN a, b, c
        |""".stripMargin
    )

    query should equal(RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set("a"),
        optionalMatches = IndexedSeq(
          QueryGraph(
            argumentIds = Set("a"),
            patternNodes = Set("a", "b"),
            patternRelationships = Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength))
          )
        )
      ),
      horizon = RegularQueryProjection(
        Map("a" -> varFor("a"), "b" -> varFor("b")),
        selections = Selections.from(greaterThan(prop("a", "prop"), prop("b", "prop")))
      ),
      tail = Some(
        RegularSinglePlannerQuery(
          QueryGraph(
            argumentIds = Set("a", "b"),
            optionalMatches = IndexedSeq(
              QueryGraph(
                argumentIds = Set("a"),
                patternNodes = Set("a", "c"),
                patternRelationships =
                  Set(PatternRelationship(v"s", (v"a", v"c"), BOTH, Seq.empty, SimplePatternLength))
              )
            )
          ),
          horizon =
            RegularQueryProjection(
              Map("a" -> varFor("a"), "b" -> varFor("b"), "c" -> varFor("c")),
              isTerminating = true
            )
        )
      )
    ))
  }

  test("should insert an extra predicate when matching on the same standalone node") {
    val query = buildSinglePlannerQuery(
      """
        |MATCH (a)-[r1]->(b)
        |WITH *, 1 AS ignore
        |MATCH (a), (b)-[r2]->(c)
        |RETURN a, b, c
        |""".stripMargin
    )

    query.allPlannerQueries.map(q => q.queryGraph.patternNodes -> q.queryGraph.selections) shouldBe Seq(
      Set("a", "b") -> Selections(),
      Set("a", "b", "c") -> Selections.from(assertIsNode("a"))
    )
  }

  private def queryWith(qg: QueryGraph, horizon: QueryHorizon = RegularQueryProjection()): PlannerQuery = {
    RegularSinglePlannerQuery(
      queryGraph = qg,
      horizon = horizon,
      tail = None
    )
  }

  test("should insert ExistsIRExpression in Selections when having a simple exists in a subquery") {
    val query = buildSinglePlannerQuery("MATCH (n)-[r]->(m) WHERE EXISTS { (o)-[r2]->(m)-[r3]->(q) } RETURN *")
    val m = varFor("m")
    val o = varFor("o")
    val q = varFor("q")
    val r2 = varFor("r2")
    val r3 = varFor("r3")

    query.queryGraph.selections.predicates.headOption.map(_.expr.asInstanceOf[ExistsIRExpression].query) shouldBe
      Some(
        queryWith(
          QueryGraph(
            patternNodes = Set(m.name, o.name, q.name),
            patternRelationships =
              Set(
                PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
                PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
              ),
            argumentIds = Set("m"),
            selections = Selections.from(Seq(
              differentRelationships(r3, r2)
            ))
          )
        )
      )
  }

  test("should insert ExistsIRExpression in Selections when having a full exists in a subquery") {
    val query = buildSinglePlannerQuery(
      "MATCH (n)-[r]->(m) WHERE EXISTS { MATCH (n) RETURN n AS name UNION MATCH (m) RETURN m AS name } RETURN *"
    )
    val m = varFor("m")
    val n = varFor("n")

    val firstQuery = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set(n.name),
        patternRelationships = Set(),
        argumentIds = Set("m", "n"),
        selections = Selections(Set(Predicate(Set("n"), assertIsNode("n"))))
      ),
      horizon = RegularQueryProjection(Map("name" -> varFor("n")))
    )
    val secondQuery = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set(m.name),
        patternRelationships = Set(),
        argumentIds = Set("m", "n"),
        selections = Selections(Set(Predicate(Set("m"), assertIsNode("m"))))
      ),
      horizon = RegularQueryProjection(Map("name" -> varFor("m")))
    )

    query.queryGraph.selections.predicates.headOption.map(_.expr.asInstanceOf[ExistsIRExpression].query) shouldBe
      Some(
        UnionQuery(
          firstQuery,
          secondQuery,
          unionMappings = List(UnionMapping(varFor("name"), varFor("name"), varFor("name"))),
          distinct = true
        )
      )
  }

  // Note that namespaced names are removed in these tests by NameDeduplication
  test("should convert a single quantified pattern") {
    val query = buildSinglePlannerQuery("MATCH ((n)-[r]->(m))+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_1"),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"anon_0"),
          rightBinding = NodeBinding(v"m", v"anon_1"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
        )
      ),
      selections = Selections.from(unique(varFor("r")))
    )
  }

  test("should convert a single quantified pattern with no node or relationship bindings") {
    val query = buildSinglePlannerQuery("MATCH (()-->())+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_4"),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"anon_5", v"anon_0"),
          rightBinding = NodeBinding(v"anon_7", v"anon_4"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"anon_6",
              (v"anon_5", v"anon_7"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"anon_5", v"anon_8"), VariableGrouping(v"anon_7", v"anon_10")),
          relationshipVariableGroupings = Set(VariableGrouping(v"anon_6", v"anon_9"))
        )
      ),
      selections = Selections.from(unique(varFor("anon_9")))
    )
  }

  test("should convert a single quantified pattern with explicitly juxtaposed nodes") {
    val query = buildSinglePlannerQuery("MATCH (a) ((n)-[r]->(m)){2, 5} (b) RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("a", "b"),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"a"),
          rightBinding = NodeBinding(v"m", v"b"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 2, max = UpperBound.Limited(5)),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
        )
      ),
      selections = Selections.from(unique(varFor("r")))
    )
  }

  test("should convert a single quantified pattern with explicitly juxtaposed patterns") {
    val query = buildSinglePlannerQuery("MATCH (a)-[r1]->(b) ((n)-[r2]->(m)){3,} (x)-[r3]->(y) RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("a", "b", "x", "y"),
      patternRelationships = Set(
        PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
        PatternRelationship(v"r3", (v"x", v"y"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      ),
      selections = Selections.from(Set(
        differentRelationships("r1", "r3"),
        unique(varFor("r2")),
        noneOfRels(varFor("r1"), varFor("r2")),
        noneOfRels(varFor("r3"), varFor("r2"))
      )),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"b"),
          rightBinding = NodeBinding(v"m", v"x"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r2",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 3, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r2", v"r2"))
        )
      )
    )
  }

  test("should convert a single quantified pattern with explicitly juxtaposed patterns with relationship types") {
    val query = buildSinglePlannerQuery("MATCH (a)-[r1:R1]->(b) ((n)-[r2:R2]->(m)){3,} (x)-[r3:R3]->(y) RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("a", "b", "x", "y"),
      patternRelationships = Set(
        PatternRelationship(
          v"r1",
          (v"a", v"b"),
          SemanticDirection.OUTGOING,
          Seq(relTypeName("R1")),
          SimplePatternLength
        ),
        PatternRelationship(
          v"r3",
          (v"x", v"y"),
          SemanticDirection.OUTGOING,
          Seq(relTypeName("R3")),
          SimplePatternLength
        )
      ),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"b"),
          rightBinding = NodeBinding(v"m", v"x"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r2",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq(relTypeName("R2")),
              SimplePatternLength
            )),
          repetition = Repetition(min = 3, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r2", v"r2"))
        )
      ),
      selections = Selections.from(unique(varFor("r2")))
    )
  }

  test("should convert consecutive quantified patterns") {
    val query = buildSinglePlannerQuery("MATCH ((n)-[r1]->(m))+ ((x)-[r2]->(y)){,3} RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_1", "anon_2"),
      selections = Selections.from(Seq(
        unique(varFor("r1")),
        unique(varFor("r2")),
        disjoint(varFor("r1"), varFor("r2"))
      )),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"anon_0"),
          rightBinding = NodeBinding(v"m", v"anon_1"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r1",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r1", v"r1"))
        ),
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"x", v"anon_1"),
          rightBinding = NodeBinding(v"y", v"anon_2"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r2",
              (v"x", v"y"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 0, max = UpperBound.Limited(3)),
          nodeVariableGroupings = Set(VariableGrouping(v"x", v"x"), VariableGrouping(v"y", v"y")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r2", v"r2"))
        )
      )
    )
  }

  test("should convert a larger consecutive quantified pattern") {
    val query = buildSinglePlannerQuery("MATCH ((a)-[r1]->(b)-[r2]->(c)){5} ((x)-[r3]->(y))+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_1", "anon_2"),
      selections = Selections.from(Seq(
        disjoint(add(varFor("r1"), varFor("r2")), varFor("r3")),
        unique(add(varFor("r1"), varFor("r2"))),
        unique(varFor("r3"))
      )),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"a", v"anon_0"),
          rightBinding = NodeBinding(v"c", v"anon_1"),
          patternRelationships =
            NonEmptyList(
              PatternRelationship(
                v"r1",
                (v"a", v"b"),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              ),
              PatternRelationship(
                v"r2",
                (v"b", v"c"),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              )
            ),
          selections = Selections.from(differentRelationships("r2", "r1")),
          repetition = Repetition(min = 5, max = UpperBound.Limited(5)),
          nodeVariableGroupings =
            Set(VariableGrouping(v"a", v"a"), VariableGrouping(v"b", v"b"), VariableGrouping(v"c", v"c")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r1", v"r1"), VariableGrouping(v"r2", v"r2"))
        ),
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"x", v"anon_1"),
          rightBinding = NodeBinding(v"y", v"anon_2"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r3",
              (v"x", v"y"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"x", v"x"), VariableGrouping(v"y", v"y")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r3", v"r3"))
        )
      )
    )
  }

  test(
    "should convert a quantified path pattern with a pattern of length > 1 whilst preserving the order in which the relationships appear"
  ) {
    val query = buildSinglePlannerQuery("MATCH ((a)-[r1]->(b)-[r2]->(c)<-[r3]-(d)){5} RETURN 1")
    val expected = QueryGraph(
      patternNodes = Set("anon_0", "anon_1"),
      selections = Selections.from(Seq(
        unique(add(add(varFor("r1"), varFor("r2")), varFor("r3")))
      )),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"a", v"anon_0"),
          rightBinding = NodeBinding(v"d", v"anon_1"),
          patternRelationships =
            NonEmptyList(
              PatternRelationship(
                v"r1",
                (v"a", v"b"),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              ),
              PatternRelationship(
                v"r2",
                (v"b", v"c"),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              ),
              PatternRelationship(
                v"r3",
                (v"c", v"d"),
                SemanticDirection.INCOMING,
                Seq.empty,
                SimplePatternLength
              )
            ),
          selections = Selections.from(List(
            differentRelationships("r3", "r2"),
            differentRelationships("r3", "r1"),
            differentRelationships("r2", "r1")
          )),
          repetition = Repetition(min = 5, max = UpperBound.Limited(5)),
          nodeVariableGroupings =
            Set(
              VariableGrouping(v"a", v"a"),
              VariableGrouping(v"b", v"b"),
              VariableGrouping(v"c", v"c"),
              VariableGrouping(v"d", v"d")
            ),
          relationshipVariableGroupings =
            Set(VariableGrouping(v"r1", v"r1"), VariableGrouping(v"r2", v"r2"), VariableGrouping(v"r3", v"r3"))
        )
      )
    )

    query.queryGraph shouldBe expected
  }

  test("should convert a single quantified pattern in OPTIONAL MATCH") {
    val query = buildSinglePlannerQuery("OPTIONAL MATCH ((n)-[r]->(m))+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      optionalMatches = Vector(QueryGraph(
        patternNodes = Set("anon_0", "anon_1"),
        quantifiedPathPatterns = Set(
          QuantifiedPathPattern(
            leftBinding = NodeBinding(v"n", v"anon_0"),
            rightBinding = NodeBinding(v"m", v"anon_1"),
            patternRelationships =
              NonEmptyList(PatternRelationship(
                v"r",
                (v"n", v"m"),
                SemanticDirection.OUTGOING,
                Seq.empty,
                SimplePatternLength
              )),
            repetition = Repetition(min = 1, max = UpperBound.Unlimited),
            nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
            relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
          )
        ),
        selections = Selections.from(unique(varFor("r")))
      ))
    )
  }

  test("should convert quantified pattern inside EXISTS subquery") {
    val query = buildSinglePlannerQuery("MATCH (a) WHERE EXISTS { (a) ((n)-[r]->(m))+ } RETURN 1")

    val m_outer = v"anon_0"
    val n = v"n"
    val r = v"r"
    val m = v"m"

    val qpp = QuantifiedPathPattern(
      leftBinding = NodeBinding(n, v"a"),
      rightBinding = NodeBinding(m, m_outer),
      patternRelationships =
        NonEmptyList(PatternRelationship(
          r,
          (n, m),
          SemanticDirection.OUTGOING,
          Seq.empty,
          SimplePatternLength
        )),
      argumentIds = Set(v"a"),
      repetition = Repetition(min = 1, max = UpperBound.Unlimited),
      nodeVariableGroupings = Set(VariableGrouping(n, n), VariableGrouping(m, m)),
      relationshipVariableGroupings = Set(VariableGrouping(r, r))
    )

    query.queryGraph.selections.predicates.headOption.map(_.expr.asInstanceOf[ExistsIRExpression].query) shouldBe
      Some(
        queryWith(
          QueryGraph(
            argumentIds = Set("a"),
            patternNodes = Set("a", m_outer.name),
            quantifiedPathPatterns = Set(qpp),
            selections = Selections.from(unique(varFor("r")))
          )
        )
      )
  }

  test("should convert a quantified pattern with a where clause") {
    val query = buildSinglePlannerQuery("MATCH ((n)-[r]->(m) WHERE n.prop > m.prop)+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_1"),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"anon_0"),
          rightBinding = NodeBinding(v"m", v"anon_1"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          selections = Selections.from(andedPropertyInequalities(greaterThan(prop("n", "prop"), prop("m", "prop")))),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
        )
      ),
      selections = Selections.from(unique(varFor("r")))
    )
  }

  test("should group inequalities when converting a quantified pattern") {
    val query = buildSinglePlannerQuery("MATCH ((n)-[r WHERE r.prop > 0 AND r.prop < 10]->(m))+ RETURN 1")
    query.queryGraph shouldBe QueryGraph(
      patternNodes = Set("anon_0", "anon_1"),
      quantifiedPathPatterns = Set(
        QuantifiedPathPattern(
          leftBinding = NodeBinding(v"n", v"anon_0"),
          rightBinding = NodeBinding(v"m", v"anon_1"),
          patternRelationships =
            NonEmptyList(PatternRelationship(
              v"r",
              (v"n", v"m"),
              SemanticDirection.OUTGOING,
              Seq.empty,
              SimplePatternLength
            )),
          selections = Selections.from(andedPropertyInequalities(
            propGreaterThan("r", "prop", 0),
            propLessThan("r", "prop", 10)
          )),
          repetition = Repetition(min = 1, max = UpperBound.Unlimited),
          nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
          relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
        )
      ),
      selections = Selections.from(unique(varFor("r")))
    )
  }

  test("should not cancel processing when clause count is below the limit") {
    val clauses = for (i <- 1 to 5) yield merge(nodePat(Some(s"n$i")))
    val query = singleQuery(clauses: _*)
    val cancellationChecker = new TestCountdownCancellationChecker(10)
    noException shouldBe thrownBy {
      StatementConverters.toPlannerQuery(
        query,
        SemanticTable(),
        new AnonymousVariableNameGenerator,
        cancellationChecker
      )
    }
  }

  test("should cancel processing when clause count is above the limit") {
    val clauses =
      (for (i <- 1 to 10) yield merge(nodePat(Some(s"n$i")))) :+
        null // should cancel before processing this bad clause

    val query = singleQuery(clauses: _*)
    val cancellationChecker = new TestCountdownCancellationChecker(10)
    val ex = the[RuntimeException] thrownBy {
      StatementConverters.toPlannerQuery(
        query,
        SemanticTable(),
        new AnonymousVariableNameGenerator,
        cancellationChecker
      )
    }
    ex should have message cancellationChecker.message
  }

  test("should cancel processing when clause count in FOREACH is above the limit") {
    val clauses =
      (for (i <- 1 to 10) yield merge(nodePat(Some(s"n$i")))) :+
        null // should cancel before processing this bad clause

    val query = singleQuery(foreach("i", varFor("list"), clauses: _*))
    val cancellationChecker = new TestCountdownCancellationChecker(10)
    val ex = the[RuntimeException] thrownBy {
      StatementConverters.toPlannerQuery(
        query,
        SemanticTable(),
        new AnonymousVariableNameGenerator,
        cancellationChecker
      )
    }
    ex should have message cancellationChecker.message
  }

  test("should cancel processing when clause count in a subquery is above the limit") {
    val clauses =
      (for (i <- 1 to 10) yield merge(nodePat(Some(s"n$i")))) :+
        null // should cancel before processing this bad clause

    val query = singleQuery(subqueryCall(clauses: _*))
    val cancellationChecker = new TestCountdownCancellationChecker(10)
    val ex = the[RuntimeException] thrownBy {
      StatementConverters.toPlannerQuery(
        query,
        SemanticTable(),
        new AnonymousVariableNameGenerator,
        cancellationChecker
      )
    }
    ex should have message cancellationChecker.message
  }

  test("should cancel processing when clause count in a union query is above the limit") {
    val clauses =
      (for (i <- 1 to 10) yield merge(nodePat(Some(s"n$i")))) :+
        null // should cancel before processing this bad clause

    val single = singleQuery(subqueryCall(clauses: _*))
    val unionQuery = ProjectingUnionAll(single, single, List.empty)(pos)
    val cancellationChecker = new TestCountdownCancellationChecker(10)
    val ex = the[RuntimeException] thrownBy {
      StatementConverters.toPlannerQuery(
        unionQuery,
        SemanticTable(),
        new AnonymousVariableNameGenerator,
        cancellationChecker
      )
    }
    ex should have message cancellationChecker.message
  }

  test("Should combine two simple create statement into one create pattern") {
    val query = buildSinglePlannerQuery(
      """CREATE (a)
        |CREATE (b)""".stripMargin
    )
    query.queryGraph shouldBe QueryGraph(
      mutatingPatterns = IndexedSeq(CreatePattern(Seq(createNodeIr("a"), createNodeIr("b"))))
    )

    query.tail shouldBe empty
  }

  test("Should combine two patterns into one") {
    val query = buildSinglePlannerQuery(
      """CREATE (a)-[r1:R {p: 1}]->(b)
        |CREATE (c)-[r2: R {p: 1}]->(d)""".stripMargin
    )
    query.queryGraph shouldBe QueryGraph(
      mutatingPatterns = IndexedSeq(CreatePattern(Seq(
        createNodeIr("a"),
        createNodeIr("b"),
        createRelationshipIr("r1", "a", "R", "b", properties = Some("{p: 1}")),
        createNodeIr("c"),
        createNodeIr("d"),
        createRelationshipIr("r2", "c", "R", "d", properties = Some("{p: 1}"))
      )))
    )

    query.tail shouldBe empty
  }

  test("should not flatten creates which have a dependency through reusing the variable") {
    val query = buildSinglePlannerQuery(
      """CREATE (m {p: 42} )
        |CREATE (n {p: m.p})""".stripMargin
    )

    query.queryGraph shouldBe QueryGraph(
      mutatingPatterns = IndexedSeq(CreatePattern(Seq(
        createNodeIr("m", properties = Some("{p: 42}")),
        createNodeIr("n", properties = Some("{p: m.p}"))
      )))
    )

    query.tail shouldBe empty
  }

  test("should not flatten creates which have a dependency through an IR expression") {
    val query = buildSinglePlannerQuery(
      """CREATE (m)
        |CREATE (n {count: COUNT { MATCH () } })""".stripMargin
    )

    query.queryGraph.mutatingPatterns should have size 2

    query.tail shouldBe empty
  }

  test("should not flatten creates which could overlap through an IR expression if merged") {
    val query = buildSinglePlannerQuery(
      """CREATE (n {count: COUNT { MATCH () } })
        |CREATE (m)""".stripMargin
    )

    query.queryGraph.mutatingPatterns should have size 2

    query.tail shouldBe empty
  }

  test("should convert query with path selector") {
    val query = buildSinglePlannerQuery(
      """MATCH SHORTEST 1 GROUP (start)((a)-[r]->(b)){1,5}(end)
        |RETURN 1 AS one""".stripMargin
    )

    val qpp: QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"a", v"start"),
        rightBinding = NodeBinding(v"b", v"end"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r",
          boundaryNodes = (v"a", v"b"),
          dir = SemanticDirection.OUTGOING,
          types = Nil,
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, UpperBound.Limited(5)),
        nodeVariableGroupings = Set(v"a", v"b").map(name => VariableGrouping(name, name)),
        relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.from(unique(varFor("r"))),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addSelectivePathPattern(shortestPathPattern)

    val projection = RegularQueryProjection(projections = Map("one" -> literalInt(1)), isTerminating = true)

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  test("should convert query with path selector and var-length relationship") {
    val query = buildSinglePlannerQuery(
      """MATCH SHORTEST 1 GROUP (start)-[r*1..5]->(end)
        |RETURN 1 AS one""".stripMargin
    )

    val rel =
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"start", v"end"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = VarPatternLength(1, Some(5))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(rel)),
        selections = Selections.from(Seq(
          unique(varFor("r")),
          varLengthLowerLimitPredicate("r", 1),
          varLengthUpperLimitPredicate("r", 5)
        )),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addSelectivePathPattern(shortestPathPattern)

    val projection = RegularQueryProjection(projections = Map("one" -> literalInt(1)), isTerminating = true)

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  test("should convert query with path selector and var-length relationship under REPEATABLE ELEMENTS") {
    val query = buildSinglePlannerQuery(
      """MATCH REPEATABLE ELEMENTS SHORTEST 1 GROUP (start)-[r*1..5]->(end)
        |RETURN 1 AS one""".stripMargin
    )

    val rel =
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"start", v"end"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = VarPatternLength(1, Some(5))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(rel)),
        selections = Selections.from(Seq(
          varLengthLowerLimitPredicate("r", 1),
          varLengthUpperLimitPredicate("r", 5)
        )),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addSelectivePathPattern(shortestPathPattern)

    val projection = RegularQueryProjection(projections = Map("one" -> literalInt(1)), isTerminating = true)

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  test("should convert query with qpp under REPEATABLE ELEMENTS") {
    val query = buildSinglePlannerQuery(
      """MATCH REPEATABLE ELEMENTS SHORTEST 1 GROUP (start)-[r*1..5]->(end)
        |RETURN 1 AS one""".stripMargin
    )

    val rel =
      PatternRelationship(
        variable = v"r",
        boundaryNodes = (v"start", v"end"),
        dir = SemanticDirection.OUTGOING,
        types = Nil,
        length = VarPatternLength(1, Some(5))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(rel)),
        selections = Selections.from(Seq(
          varLengthLowerLimitPredicate("r", 1),
          varLengthUpperLimitPredicate("r", 5)
        )),
        selector = SelectivePathPattern.Selector.ShortestGroups(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addSelectivePathPattern(shortestPathPattern)

    val projection = RegularQueryProjection(projections = Map("one" -> literalInt(1)), isTerminating = true)

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  test("should convert query with path selector, path assignment, and predicates") {
    val query = buildSinglePlannerQuery(
      """MATCH p = ANY SHORTEST ((start:Start)((a)-[r:R]->(b))+(end) WHERE start.prop < size(r))
        |WHERE end.prop > 0
        |RETURN p, start, r""".stripMargin
    )

    val qpp: QuantifiedPathPattern =
      QuantifiedPathPattern(
        leftBinding = NodeBinding(v"a", v"start"),
        rightBinding = NodeBinding(v"b", v"end"),
        patternRelationships = NonEmptyList(PatternRelationship(
          variable = v"r",
          boundaryNodes = (v"a", v"b"),
          dir = SemanticDirection.OUTGOING,
          types = List(relTypeName("R")),
          length = SimplePatternLength
        )),
        argumentIds = Set.empty,
        selections = Selections.empty,
        repetition = Repetition(1, UpperBound.Unlimited),
        nodeVariableGroupings = Set(v"a", v"b").map(name => VariableGrouping(name, name)),
        relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
      )

    val shortestPathPattern =
      SelectivePathPattern(
        pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp)),
        selections = Selections.from(List(
          andedPropertyInequalities(lessThan(prop("start", "prop"), function("size", varFor("r")))),
          unique(varFor("r"))
        )),
        selector = SelectivePathPattern.Selector.Shortest(1)
      )

    val queryGraph =
      QueryGraph
        .empty
        .addPredicates(
          andedPropertyInequalities(greaterThan(prop("end", "prop"), literalInt(0))),
          hasLabels("start", "Start")
        )
        .addSelectivePathPattern(shortestPathPattern)

    val projection = RegularQueryProjection(
      projections = Map(
        "p" -> PathExpression(
          step = NodePathStep(
            node = varFor("start"),
            next = RepeatPathStep(
              variables = List(NodeRelPair(varFor("a"), varFor("r"))),
              toNode = varFor("end"),
              next = NilPathStep()(pos)
            )(pos)
          )(pos)
        )(pos),
        "start" -> varFor("start"),
        "r" -> varFor("r")
      ),
      isTerminating = true
    )

    query shouldEqual RegularSinglePlannerQuery(queryGraph = queryGraph, horizon = projection)
  }

  // Interior SHORTEST PATH overlap

  test("should keep two query graphs separate with shortest with relationship var followed by exhaustive") {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ ()-[r]-() (()--())+
        |MATCH (a)-[r]-(b)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test("should keep two query graphs separate with exhaustive followed by shortest with relationship var") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)-[r]-(b)
        |MATCH ANY SHORTEST (()--())+ ()-[r]-() (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should keep two query graphs separate with shortest with relationship var followed by shortest with relationship var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (a)-[r]-(b) (()--())+
        |MATCH ANY SHORTEST (()--())+ ()-[r]-() (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test("should keep two query graphs separate with shortest with interior node var followed by exhaustive") {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |MATCH (a)-[r]-(b)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test("should keep two query graphs separate with exhaustive followed by shortest with interior node var") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)-[r]-(b)
        |MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should keep two query graphs separate with shortest with interior node var followed by shortest with interior node var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should keep two query graphs separate with shortest with interior node var followed by shortest with boundary node var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |MATCH ANY SHORTEST (()--())+ (()--())+ (a)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should keep two query graphs separate with shortest with boundary node var followed by shortest with interior node var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (()--())+ (a)
        |MATCH ANY SHORTEST (()--())+ (a) (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should squash two query graphs with shortest with boundary node var followed by shortest with boundary node var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (()--())+ (a)
        |MATCH ANY SHORTEST (a) (()--())+ (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("should squash two query graphs with shortest with right boundary node var followed by exhaustive") {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (()--())+ (a)
        |MATCH (a)-[r]-(b)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("should squash two query graphs with exhaustive followed by shortest with right boundary node var") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)-[r]-(b)
        |MATCH ANY SHORTEST (()--())+ (()--())+ (a)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("should squash two query graphs with shortest with left boundary node var followed by exhaustive") {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (a) (()--())+ (()--())+
        |MATCH (a)-[r]-(b)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("should squash two query graphs with exhaustive followed by shortest with left boundary node var") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)-[r]-(b)
        |MATCH ANY SHORTEST (a) (()--())+ (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("should squash two query graphs with shortest followed by exhaustive") {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST (()--())+ (()--())+
        |MATCH (a)-[r]-(b)
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test("hould squash two query graphs with exhaustive followed by shortest") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)-[r]-(b)
        |MATCH ANY SHORTEST (()--())+ (()--())+
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 1
  }

  test(
    "should keep two query graphs separate with exhaustive with relationship group var followed by shortest with var-length relationship var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ((a)-[r]-(b))+
        |MATCH ANY SHORTEST ()--()-[r*]-()--()
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  test(
    "should keep two query graphs separate with shortest with relationship group var followed by exhaustive with var-length relationship var"
  ) {
    val query = buildSinglePlannerQuery(
      """MATCH ANY SHORTEST ((a)-[r]-(b))+
        |MATCH ()--()-[r*]-()--()
        |RETURN *""".stripMargin
    )

    query.allPlannerQueries should have size 2
  }

  private class TestCountdownCancellationChecker(var count: Int) extends CancellationChecker {
    val message = "my exception"

    override def throwIfCancelled(): Unit = {
      count -= 1
      if (count <= 0) throw new RuntimeException(message)
    }
  }

  private def createNodeIr(node: String, properties: Option[String] = None): org.neo4j.cypher.internal.ir.CreateNode =
    org.neo4j.cypher.internal.ir.CreateNode(varFor(node), Set.empty, properties.map(Parser.parseExpression))

  private def createRelationshipIr(
    relationship: String,
    left: String,
    typ: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[String]
  ): org.neo4j.cypher.internal.ir.CreateRelationship = {
    val props = properties.map(Parser.parseExpression)
    if (props.exists(!_.isInstanceOf[MapExpression]))
      throw new IllegalArgumentException("Property must be a Map Expression")
    org.neo4j.cypher.internal.ir.CreateRelationship(
      varFor(relationship),
      varFor(left),
      RelTypeName(typ)(pos),
      varFor(right),
      direction,
      properties.map(Parser.parseExpression)
    )
  }
}
