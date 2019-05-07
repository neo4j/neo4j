/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.ir.{QueryGraph, RegularPlannerQuery}
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
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternRelationships.size == 1 => 10
        // argument
        case RegularPlannerQuery(queryGraph, _, _, _) if containsArgumentOnly(queryGraph) => 1
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
          in(prop("a", "prop2"), listOfInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate and an expression") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          in(prop("a", "prop"), listOfInt(9))
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
          in(prop("a", "prop"), listOfInt(9))
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
                  In(Property(Variable("r_NODES"), PropertyKeyName("prop") ), ListLiteral(List(SignedDecimalIntegerLiteral("1337"))))
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
                  Not(In(Property(Variable("r_NODES"), PropertyKeyName("prop") ), ListLiteral(List(SignedDecimalIntegerLiteral("1337")))))
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
      Ands(SetExtractor(In(Property(Variable("n"), PropertyKeyName("prop")), _))),
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
      Ands(SetExtractor(In(Property(Variable("n"), PropertyKeyName("prop")), _))),
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
      )=> ()
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
      )=> ()
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
      )=> ()
    }
  }

  private def containsArgumentOnly(queryGraph: QueryGraph): Boolean =
    queryGraph.argumentIds.nonEmpty && queryGraph.patternNodes.isEmpty && queryGraph.patternRelationships.isEmpty
}
