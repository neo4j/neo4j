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

import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.ir.{QueryGraph, RegularSinglePlannerQuery}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.test_helpers.Extractors.{MapKeys, SetExtractor}

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
          case ListComprehension(ExtractScope(_, Some(NestedPlanExpression(nestedPlan, _)), _), _) =>
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
              NodeByLabelScan("u", LabelName("User")(pos), Set.empty),
              Projection(
                Selection(
                  Seq(hasLabels("u2", "User")),
                  CacheProperties(
                    Expand(
                      Argument(Set("u")),
                      "u",
                      OUTGOING,
                      Seq(RelTypeName("FOLLOWS")(pos)),
                      "u2",
                      "r"
                    ),
                    Set(CachedProperty("u", Variable("u")(pos), PropertyKeyName("id")(pos), NODE_TYPE)(pos))
                  )
                ),
                Map("  FRESHID41" -> prop("u2", "id"))
              ),
              "  FRESHID42",
              "  FRESHID41",
              Set("u")
            ),
            Map(patternComprehensionExpressionKeyString -> function("size", Variable("  FRESHID42")(pos)))
          ),
          Seq(Ascending(patternComprehensionExpressionKeyString))
        ),
        Map("u.id" -> cachedNodeProp("u", "id"))
      )
    )
  }

  test("should build plans with getDegree for a single pattern predicate") {
    planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X") _), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(
          lessThanOrEqual(GetDegree(varFor("a"),Some(RelTypeName("X") _), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a")._2 should equal(
      Selection(
        ands(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a pattern predicate and an expression") {
    planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          propGreaterThan("a", "prop", 4)
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a pattern predicate and multiple expressions") {
    planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          propGreaterThan("a", "prop", 4),
          equals(prop("a", "prop2"), literalInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate and an expression") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          equals(prop("a", "prop"), literalInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates and expressions") {
    planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0)),
          equals(prop("a", "prop"), literalInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates with one negation") {
    planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two negated pattern predicates") {
    planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        )),  AllNodesScan("a", Set.empty)
      )
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
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
    val (_, plan, _, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
    val (_, plan, _, _, _) = new given {
      uniqueIndexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
                 RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
    val (_, plan, _, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
    val (_, plan, _, _, _) = new given {
      indexOn("Label", "prop")
    } getLogicalPlanFor q

    plan should beLike {
      case Apply(
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, collectionName, _, _),
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
      Ands(SetExtractor(Equals(Property(Variable("n"), PropertyKeyName("prop")), _))),
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
      Ands(SetExtractor(Equals(Property(Variable("n"), PropertyKeyName("prop")), _))),
      RollUpApply(Projection(AllNodesScan("n", SetExtractor()), MapKeys("one")), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the (a)-->(b) subQuery */, _, _, _),
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
      case RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _, _) => ()
    }
  }

  test("should solve and name pattern expressions for Projection") {
    val q =
      """
        |MATCH (n)
        |RETURN (n)-->() AS bs
      """.stripMargin

    planFor(q)._2 should beLike {
      case RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "bs", _, _) => ()
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
        RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _, _),
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
        RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", _), _/* <- This is the subQuery */, "ages", _, _),
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
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
                         RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _, _) // Match part
                        ), _
              ),
      MergeCreateNode(
                      RollUpApply(
                        CacheProperties(Argument(SetExtractor()), _),
                      _/* <- This is the subQuery */, _, _, _), _,_ ,_ // Create part
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
                    CacheProperties(
                         RollUpApply(
                                     Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _), _/* <- This is the subQuery */, _, _, _) // Match part
                    , _)
               ), _
              ),
      MergeCreateRelationship(
                      RollUpApply(
                                  _: MergeCreateNode, _/* <- This is the subQuery */, _, _, _), _,_ ,_, _, _ // Create part
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
        DeleteNode(_, ContainerIndex(Variable("nodes"), ReduceExpression(_, _, NestedPlanExpression(_, _))))
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
        DeleteRelationship(_, ContainerIndex(Variable("rels"), ReduceExpression(_, _, NestedPlanExpression(_, _))))
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
          DeleteExpression(_, ContainerIndex(Variable("rels"), FunctionInvocation(_, _, _, Vector(ReduceExpression(_, _, NestedPlanExpression(_, _))))
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(AllNodesScan("n", SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _), _/* <- This is the subQuery */, _, _, _),
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
      RollUpApply(Expand(AllNodesScan(_, SetExtractor()), _, _, _, _, _, _), _/* <- This is the subQuery */, _, _, _),
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
                                   RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
                                   RollUpApply(Argument(SetExtractor()), _/* <- This is the subQuery */, _, _, _),
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
