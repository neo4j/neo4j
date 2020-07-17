/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.MapKeys
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should consider variables introduced by outer list comprehensions when planning pattern predicates") {
    val plan = (new given {
      cardinality = mapCardinality {
        // expand
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternRelationships.size == 1 => 10
        // argument
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if containsArgumentOnly(queryGraph) => 1
        case _ => 4000000
      }
    } getLogicalPlanFor """MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a, collect(b) AS friends RETURN a, [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS clowns""")._2

    plan match {
      case Projection(_, expressions) =>
        expressions("clowns") match {
          case ListComprehension(ExtractScope(_, Some(NestedPlanExistsExpression(nestedPlan, _)), _), _) =>
            nestedPlan should equal(
              Selection(
                ands(hasLabels("  NODE116", "ComedyClub")),
                Expand(
                  Argument(Set("f")),
                  "f", OUTGOING, Seq(RelTypeName("WORKS_AT")_), "  NODE116", "  REL102", ExpandAll
                )
              )
            )
        }
    }
  }

  test("should get the right scope for pattern comprehensions in ORDER BY") {
    // The important thing for this test is "RETURN u.id" instead of "RETURN u".
    // Like this the scoping is challenged to propagate `u` from the previous scope into the pattern expression

    val patternComprehensionExpressionKeyString = "size(PatternComprehension(None,RelationshipsPattern(RelationshipChain(NodePattern(Some(Variable(u)),List(),None,None),RelationshipPattern(Some(Variable(r)),List(RelTypeName(FOLLOWS)),None,None,OUTGOING,false,None),NodePattern(Some(Variable(u2)),List(LabelName(User)),None,None))),None,Property(Variable(u2),PropertyKeyName(id))))"

    planFor("MATCH (u:User) RETURN u.id ORDER BY size([(u)-[r:FOLLOWS]->(u2:User) | u2.id])")._2 should equal(
      Projection(
        Sort(
          Projection(
            RollUpApply(
              NodeByLabelScan("u", LabelName("User")(pos), Set.empty, IndexOrderNone),
              Projection(
                Selection(
                  Seq(hasLabels("u2", "User")),
                  Expand(
                    Argument(Set("u")),
                    "u",
                    OUTGOING,
                    Seq(RelTypeName("FOLLOWS")(pos)),
                    "u2",
                    "r"
                  )
                ),
                Map("  FRESHID41" -> prop("u2", "id"))
              ),
              "  FRESHID42",
              "  FRESHID41"
            ),
            Map(patternComprehensionExpressionKeyString -> function("size", Variable("  FRESHID42")(pos)))
          ),
          Seq(Ascending(patternComprehensionExpressionKeyString))
        ),
        Map("u.id" -> prop("u", "id"))
      )
    )
  }

  // Please look at the SemiApplyVsGetDegree benchmark.
  // GetDegree is slower on sparse nodes, but faster on dense nodes.
  // We heuristically always choose SemiApply, which will do better on average.

  test("should build plans with SemiApply for a single pattern predicate") {
    val logicalPlan = planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[`  REL20`:X]->(`  NODE27`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate") {
    val logicalPlan = planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[`  REL24`:X]->(`  NODE31`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with exists") {
    val logicalPlan = planFor("MATCH (a) WHERE exists((a)-[:X]->()) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[`  REL27`:X]->(`  NODE34`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with exists") {
    val logicalPlan = planFor("MATCH (a) WHERE NOT exists((a)-[:X]->()) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[`  REL31`:X]->(`  NODE38`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with size") {
    val logicalPlan = planFor("MATCH (a) WHERE size((a)-[:X]->())>0 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[`  REL25`:X]->(`  NODE32`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with size") {
    val logicalPlan = planFor("MATCH (a) WHERE size((a)-[:X]->())=0 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.expandAll("(a)-[`  REL25`:X]->(`  NODE32`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with 2 SemiApplies for two pattern predicates") {
    val logicalPlan = planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.expandAll("(a)-[`  REL37`:Y]->(`  NODE44`)")
        .|.argument("a")
        .semiApply()
        .|.expandAll("(a)-[`  REL20`:X]->(`  NODE27`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrSemiApply for a pattern predicate and an expression") {
    val logicalPlan = planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrSemiApply("a.prop > 4")
        .|.expandAll("(a)-[`  REL20`:X]->(`  NODE27`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrSemiApply for a pattern predicate and multiple expressions") {
    val logicalPlan = planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrSemiApply("a.prop > 4 OR a.prop2 = 9")
        .|.expandAll("(a)-[`  REL35`:X]->(`  NODE42`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SelectOrAntiSemiApply for a single negated pattern predicate and an expression") {
    val logicalPlan = planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("a.prop = 9")
        .|.expandAll("(a)-[`  REL38`:X]->(`  NODE45`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with LetSelectOrSemiApply and SelectOrAntiSemiApply for two pattern predicates and expressions") {
    val logicalPlan = planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("`  FRESHID30`")
        .|.expandAll("(a)-[`  REL54`:X]->(`  NODE61`)")
        .|.argument("a")
        .letSelectOrSemiApply("  FRESHID30", "a.prop = 9") // FRESHID30 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[`  REL34`:Y]->(`  NODE41`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with LetSemiApply and SelectOrAntiSemiApply for two pattern predicates with one negation") {
    val logicalPlan = planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("`  FRESHID16`")
        .|.expandAll("(a)-[`  REL40`:X]->(`  NODE47`)")
        .|.argument("a")
        .letSemiApply("  FRESHID16") // FRESHID16 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[`  REL20`:Y]->(`  NODE27`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with LetAntiSemiApply and SelectOrAntiSemiApply for two negated pattern predicates") {
    val logicalPlan = planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .selectOrAntiSemiApply("`  FRESHID20`")
        .|.expandAll("(a)-[`  REL44`:X]->(`  NODE51`)")
        .|.argument("a")
        .letAntiSemiApply("  FRESHID20") // FRESHID20 is used to not go into the RHS of SelectOrAntiSemiApply if not needed
        .|.expandAll("(a)-[`  REL24`:Y]->(`  NODE31`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE (a)-[:X]->(:Foo) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("`  NODE27`:Foo")
        .|.expandAll("(a)-[`  REL20`:X]->(`  NODE27`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE NOT (a)-[:X]->(:Foo) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("`  NODE31`:Foo")
        .|.expandAll("(a)-[`  REL24`:X]->(`  NODE31`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with exists with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE exists((a)-[:X]->(:Foo)) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("`  NODE34`:Foo")
        .|.expandAll("(a)-[`  REL27`:X]->(`  NODE34`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with exists with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE NOT exists((a)-[:X]->(:Foo)) RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("`  NODE38`:Foo")
        .|.expandAll("(a)-[`  REL31`:X]->(`  NODE38`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with SemiApply for a single pattern predicate with size with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE size((a)-[:X]->(:Foo))>0 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .semiApply()
        .|.filter("`  NODE32`:Foo")
        .|.expandAll("(a)-[`  REL25`:X]->(`  NODE32`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with AntiSemiApply for a single negated pattern predicate with size with Label on other node") {
    val logicalPlan = planFor("MATCH (a) WHERE size((a)-[:X]->(:Foo))=0 RETURN a", stripProduceResults = false)._2
    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a")
        .antiSemiApply()
        .|.filter("`  NODE32`:Foo")
        .|.expandAll("(a)-[`  REL25`:X]->(`  NODE32`)")
        .|.argument("a")
        .allNodeScan("a")
        .build()
    )
  }

  test("should plan all predicates along with named varlength pattern") {
    planFor("MATCH p=(a)-[r*]->(b) WHERE all(n in nodes(p) WHERE n.prop = 1337) RETURN p")._2 should beLike {
      case Projection(
      VarExpand(_, _, _, _, _, _, _, _, _,
                Some(VariablePredicate(
                  Variable("r_NODES"),
                  Equals(Property(Variable("r_NODES"), PropertyKeyName("prop") ), SignedDecimalIntegerLiteral("1337"))
                )),
                _), _) => ()

    }
  }

  test("should plan none predicates along with named varlength pattern") {
    planFor("MATCH p=(a)-[r*]->(b) WHERE none(n in nodes(p) WHERE n.prop = 1337) RETURN p")._2 should beLike {
      case Projection(
      VarExpand(_, _, _, _, _, _, _, _, _,
                Some(VariablePredicate(
                  Variable("r_NODES"),
                  Not(Equals(Property(Variable("r_NODES"), PropertyKeyName("prop") ), SignedDecimalIntegerLiteral("1337")))
                )),
                _), _) => ()

    }
  }

  test("should solve pattern comprehensions as part of VarExpand") {
    val q =
      """
        |MATCH p= ( (b) -[:REL*0..]- (c) )
        |WHERE
        | ALL(n in nodes(p) where
        |   n.prop <= 1 < n.prop2
        |   AND coalesce(
        |     head( [ (n)<--(d) WHERE d.prop3 <= 1 < d.prop2 | d.prop4 = true ] ),
        |     head( [ (n)<--(e) WHERE e.prop3 <= 1 < e.prop2 | e.prop5 = '0'] ),
        |     true)
        | )
        | AND ALL(r in relationships(p) WHERE r.prop <= 1 < r.prop2)
        |RETURN c
      """.stripMargin

    planFor(q) // Should not fail
    // The plan that solves the predicates as part of VarExpand is not chosen, but considered, thus we cannot assert on _that_ plan here.
    // Nevertheless the assertion LogicalPlanProducer.assertNoBadExpressionsExists should never fail, even for plans that do not get chosen.

  }

  test("should not use RollupApply for PatternComprehensions in coalesce") {
    val q =
      """
        |MATCH (a)
        |WHERE coalesce(
        |     head( [ (a)<--(b) | b.prop4 = true ] ),
        |     head( [ (a)<--(c) | c.prop5 = '0'] ),
        |     true)
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in coalesce v2") {
    val q =
      """
        |MATCH (a)
        |WHERE coalesce(
        |     [1, 2, 3],
        |     [(a)<--(c) | c.prop5 = '0'],
        |     [true])
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    println(plan)
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  test("should solve pattern comprehension for NodeByIdSeek") {
    val q =
      """
        |MATCH (n)
        |WHERE id(n) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
                 NodeByIdSeek("n", _, SetExtractor(argumentName))
                ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for DirectedRelationshipByIdSeek") {
    val q =
      """
        |MATCH ()-[r]->()
        |WHERE id(r) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN r
      """.stripMargin

    planFor(q)._2 should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
                 DirectedRelationshipByIdSeek("r", _, _, _, SetExtractor(argumentName))
                ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for UndirectedRelationshipByIdSeek") {
    val q =
      """
        |MATCH ()-[r]-()
        |WHERE id(r) = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN r
      """.stripMargin

    planFor(q)._2 should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
                 UndirectedRelationshipByIdSeek("r", _, _, _, SetExtractor(argumentName))
                ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for NodeIndexSeek") {
    val q =
      """
        |MATCH (n:Label)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin
    val (_, plan, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
                 NodeIndexSeek("n", _, _, _, SetExtractor(argumentName), _)
                ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for NodeUniqueIndexSeek") {
    val q =
      """
        |MATCH (n:Label)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin
    val (_, plan, _, _) = new given {
      uniqueIndexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
                 NodeUniqueIndexSeek("n", _, _, _, SetExtractor(argumentName), _)
                ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for NodeIndexContainsScan") {
    val q =
      """
        |MATCH (n:Label)
        |WHERE n.prop CONTAINS toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN n
      """.stripMargin
    val (_, plan, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
      NodeIndexContainsScan("n", _, _, _, SetExtractor(argumentName), _)
      ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for NodeIndexEndsWithScan") {
    val q =
      """
        |MATCH (n:Label)
        |WHERE n.prop ENDS WITH toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))
        |RETURN n
      """.stripMargin
    val (_, plan, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _),
      NodeIndexEndsWithScan("n", _, _, _, SetExtractor(argumentName), _)
      ) if collectionName == argumentName => ()
    }
  }

  test("should solve pattern comprehension for Selection") {
    val q =
      """
        |MATCH (n)
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case Selection(
      Ands(Seq(Equals(Property(Variable("n"), PropertyKeyName("prop")), _))),
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _),
      ) => ()
    }
  }

  test("should solve pattern comprehension for Horizon Selection") {
    val q =
      """
        |MATCH (n)
        |WITH n, 1 AS one
        |WHERE n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case Selection(
      Ands(Seq(Equals(Property(Variable("n"), PropertyKeyName("prop")), _))),
      RollUpApply(Projection(AllNodesScan("n", SetExtractor()), MapKeys("one"))/* <- This is the subQuery */, _, _, _),
      ) => ()
    }
  }

  test("should solve pattern comprehension for SelectOrAntiSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE NOT (n)-[:R]->(:M) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SelectOrAntiSemiApply(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _),
      _ /* <- This is the (n)-[:R]->(:M) subQuery */,
      _
      ) => ()
    }
  }

  test("should solve pattern comprehension for LetSelectOrAntiSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE NOT (n)-[:R]->(:M) OR NOT (n)-[:Q]->(:O) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SelectOrAntiSemiApply(
      LetSelectOrAntiSemiApply(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _),
      _ /* <- This is the (n)-[:R]->(:M) subQuery */,
      _,
      _
      ),
      _ /* <- This is the (n)-[:Q]->(:O) subQuery */,
      _
      ) => ()
    }
  }

  test("should solve pattern comprehension for SelectOrSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE (n)-[:R]->(:M) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SelectOrSemiApply(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _),
      _ /* <- This is the (n)-[:R]->(:M) subQuery */,
      _
      ) => ()
    }
  }

  test("should solve pattern comprehension for LetSelectOrSemiApply") {
    val q =
      """
        |MATCH (n)
        |WHERE (n)-[:R]->(:M) OR (n)-[:Q]->(:O) OR n.prop = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
        |RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SelectOrSemiApply(
      LetSelectOrSemiApply(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _),
      _ /* <- This is the (n)-[:R]->(:M) subQuery */,
      _,
      _
      ),
      _ /* <- This is the (n)-[:Q]->(:O) subQuery */,
      _
      ) => ()
    }
  }

  test("should solve and name pattern comprehensions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | b.age] AS ages
      """.stripMargin

    planFor(q)._2 should beLike {
      case RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _) => ()
    }
  }

  test("should solve and name pattern expressions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN (n)-->() AS bs
      """.stripMargin

    planFor(q)._2 should beLike {
      case RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "bs", _) => ()
    }
  }

  test("should solve and name pattern comprehensions for Aggregation, grouping expression") {
    val q =
      """
        |MATCH (n)
        |RETURN [(n)-->(b) | b.age] AS ages, sum(n.foo)
      """.stripMargin

    planFor(q)._2 should beLike {
      case Aggregation(
        RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _),
      _,
      _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for Aggregation, aggregation expression") {
    val q =
      """
        |MATCH (n)
        |RETURN collect([(n)-->(b) | b.age]) AS ages, n.foo
      """.stripMargin

    planFor(q)._2 should beLike {
      case Aggregation(
        RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, _, _),
      _,
      _
      ) => ()
    }
  }

  test("should solve and name pattern comprehensions for Distinct") {
    val q =
      """
        |MATCH (n)
        |RETURN DISTINCT [(n)-->(b) | b.age] AS ages
      """.stripMargin

    planFor(q)._2 should beLike {
      case Distinct(
      RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _),
      _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for LoadCSV") {
    val q =
      """
        |LOAD CSV FROM toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)) AS foo RETURN foo
      """.stripMargin

    planFor(q)._2 should beLike {
      case LoadCSV(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _, _, _, _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for Unwind") {
    val q =
      """
        |UNWIND [reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)] AS foo RETURN foo
      """.stripMargin

    planFor(q)._2 should beLike {
      case UnwindCollection(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _),
      "foo",
      _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for ShortestPath") {
    val q =
      """
        |MATCH p=shortestPath((n)-[r*..6]-(n2)) WHERE NONE(n in nodes(p) WHERE n.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)) RETURN n, n2
      """.stripMargin

    // This will be solved with a NestedPlanExpression instead of RollupApply
    planFor(q)._2 should beLike {
      case FindShortestPaths(_, _, Seq(
      NoneIterablePredicate(FilterScope(_, Some(Equals(_, ReduceExpression(_, _, _:NestedPlanExpression)))), _)
      ), _, _) => ()
    }
  }

  test("should solve pattern comprehensions for Create") {
    val q =
      """
        |CREATE (n {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}) RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case Create(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for MergeCreateNode") {
    val q =
      """
        |MERGE (n {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}) RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case AntiConditionalApply(
      Optional(
               Selection(_,
                         RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _) // Match part
                        ), _
              ),
      MergeCreateNode(
                      RollUpApply(
                        Argument(SetExtractor()),
                      _/* <- This is the subQuery */, _, _), _,_ ,_ // Create part
                     ), _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for MergeCreateRelationship") {
    val q =
      """
        |MERGE ()-[r:R {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)}]->() RETURN r
      """.stripMargin

    planFor(q)._2 should beLike {
      case AntiConditionalApply(
      Optional(
               Selection(_,
                       RollUpApply(
                          Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _, _), _/* <- This is the subQuery */, _, _) // Match part
               ), _
              ),
      MergeCreateRelationship(
                      RollUpApply(
                                  _: MergeCreateNode, _/* <- This is the subQuery */, _, _), _,_ ,_, _, _ // Create part
                     ), _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for DeleteNode") {
    val q =
      """
        |MATCH (n)
        |WITH [n] AS nodes
        |DELETE nodes[reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)]
      """.stripMargin

    planFor(q)._2 should beLike {
      case EmptyResult(
        DeleteNode(_, ContainerIndex(Variable("nodes"), ReduceExpression(_, _, NestedPlanCollectExpression(_, _, _))))
      ) => ()
    }
  }

  test("should solve pattern comprehensions for DeleteRelationship") {
    val q =
      """
        |MATCH ()-[r]->()
        |WITH [r] AS rels
        |DELETE rels[reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)]
      """.stripMargin

    planFor(q)._2 should beLike {
      case EmptyResult(
        DeleteRelationship(_, ContainerIndex(Variable("rels"), ReduceExpression(_, _, NestedPlanCollectExpression(_, _, _))))
      ) => ()
    }
  }

  test("should solve pattern comprehensions for DeleteExpression") {
    val q =
      """
        |MATCH ()-[r]->()
        |WITH {rel: r} AS rels
        |DELETE rels[toString(reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x))]
      """.stripMargin

    planFor(q)._2 should beLike {
      case EmptyResult(
        Apply(_,
          DeleteExpression(_, ContainerIndex(Variable("rels"), FunctionInvocation(_, _, _, Vector(ReduceExpression(_, _, NestedPlanCollectExpression(_, _, _))))
      )))) => ()
    }
  }

  test("should solve pattern comprehensions for SetNodeProperty") {
    val q =
      """
        |MATCH (n) SET n.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x) RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SetNodeProperty(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for SetNodePropertiesFromMap") {
    val q =
      """
        |MATCH (n) SET n = {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)} RETURN n
      """.stripMargin

    planFor(q)._2 should beLike {
      case SetNodePropertiesFromMap(
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for SetRelationshipProperty") {
    val q =
      """
        |MATCH ()-[r]->() SET r.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x) RETURN r
      """.stripMargin

    planFor(q)._2 should beLike {
      case SetRelationshipProperty(
      RollUpApply(Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _, _), _/* <- This is the subQuery */, _, _),
      _, _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for SetRelationshipPropertiesFromMap") {
    val q =
      """
        |MATCH ()-[r]->() SET r = {foo: reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)} RETURN r
      """.stripMargin

    planFor(q)._2 should beLike {
      case SetRelationshipPropertiesFromMap(
      RollUpApply(Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _, _), _/* <- This is the subQuery */, _, _),
      _, _, _
      ) => ()
    }
  }

  test("should solve pattern comprehensions for SetProperty") {
    val q =
      """
        |SET $param.foo = reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
      """.stripMargin

    planFor(q)._2 should beLike {
      case EmptyResult(
                       SetProperty(
                                   RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _, _
      )) => ()
    }
  }

  test("should solve pattern comprehensions for ForeachApply") {
    val q =
      """
        |FOREACH (num IN [reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)] |
        |  CREATE ({foo: num}) )
      """.stripMargin

    planFor(q)._2 should beLike {
      case EmptyResult(
                       ForeachApply(
                                   RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _),
      _, _, _
      )) => ()
    }
  }

  test("should not use RollupApply for PatternComprehensions in head") {
    val q =
      """
        |MATCH (a)
        |WHERE head( [(a)<--(b) | b.prop4 = true] ) = true
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in container index") {
    val q =
      """
        |MATCH (a)
        |WHERE [(a)<--(b) | b.prop4 = true][2]
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in list slice to") {
    val q =
      """
        |MATCH (a)
        |WHERE [(a)<--(b) | b.prop4 = true][..5]
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  test("should not use RollupApply for PatternComprehensions in list slice from/to") {
    val q =
      """
        |MATCH (a)
        |WHERE [ (a)<--(b) | b.prop4 = true ][2..5]
        |RETURN a
      """.stripMargin

    val plan = planFor(q)._2
    plan.treeExists({
      case _:RollUpApply => true
    }) should be(false)
  }

  private def containsArgumentOnly(queryGraph: QueryGraph): Boolean =
    queryGraph.argumentIds.nonEmpty && queryGraph.patternNodes.isEmpty && queryGraph.patternRelationships.isEmpty
}
