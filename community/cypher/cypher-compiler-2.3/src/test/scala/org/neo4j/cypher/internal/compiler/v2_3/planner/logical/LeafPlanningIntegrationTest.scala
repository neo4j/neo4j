/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, RangeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.{LabelId, PropertyKeyId}

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should use index seek for simple prefix search using like with %") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'prefix%' RETURN a").innerPlan should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
        RangeQueryExpression(StringSeekRange(LowerBounded(InclusiveBound("prefix"))) _),
        Set.empty)(solved)
    )
  }

  test("should not use index seek for empty prefix search using like with %") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE '%' RETURN a").innerPlan should equal(
      Selection(Seq(Like(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                         LikePattern(StringLiteral("%") _)) _),
                NodeByLabelScan("a", LazyLabel("Person"), Set.empty)(solved)
      )(solved))
  }

  test("should use index prefix search for multiple LIKEs with AND, and choose the longer prefix") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'short%' AND a.lastname LIKE 'longer%' RETURN a")
      .innerPlan should equal(
      Selection(Seq(Like(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                         LikePattern(StringLiteral("short%") _)) _),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("lastname") _, PropertyKeyId(1)),
                  RangeQueryExpression(StringSeekRange(LowerBounded(InclusiveBound("longer"))) _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should use index prefix search for multiple LIKEs with AND NOT") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'longer%' AND NOT a.lastname LIKE 'short%' RETURN a")
      .innerPlan should equal(
      Selection(Seq(Not(Like(Property(Identifier("a") _, PropertyKeyName("lastname") _) _,
                             LikePattern(StringLiteral("short%") _)) _) _),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  RangeQueryExpression(StringSeekRange(LowerBounded(InclusiveBound("longer"))) _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should use index seek for complex prefix search using like with %") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'prefix%suffix' RETURN a").innerPlan should equal(
      Selection(
        Seq(Like(Property(ident("a"), PropertyKeyName("name")_)_, LikePattern(StringLiteral("prefix%suffix")_))_),
        NodeIndexSeek(
          "a",
          LabelToken("Person", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
          RangeQueryExpression(StringSeekRange(LowerBounded(InclusiveBound("prefix"))) _),
          Set.empty)(solved)
      )(solved)
    )
  }

  test("should prefer property equality index seek over range index seek") {
    val like: Like = Like(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                          LikePattern(StringLiteral("prefix%") _)) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'prefix%' AND a.name = 'prefix1' RETURN a").innerPlan should equal(
      Selection(Seq(like),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  SingleQueryExpression(StringLiteral("prefix1") _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should prefer property equality index seek over collection over range index seek") {
    val like: Like = Like(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                          LikePattern(StringLiteral("prefix%") _)) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name LIKE 'prefix%' AND a.name in ['prefix1', 'prefix2'] RETURN a").innerPlan should equal(
      Selection(Seq(like),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  ManyQueryExpression(Collection(List(StringLiteral("prefix1") _, StringLiteral("prefix2") _)) _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should build plans for all nodes scans") {
    (new given {
    } planFor "MATCH (n) RETURN n").innerPlan should equal(
      AllNodesScan("n", Set.empty)(solved)
    )
  }

  test("should build plans for label scans without compile-time label id") {
    (new given {
      cost =  {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByIdSeek, _) => 2.0
        case (_: NodeByLabelScan, _) => 1.0
        case _ => Double.MaxValue
      }
    } planFor "MATCH (n:Awesome) RETURN n").innerPlan should equal(
      NodeByLabelScan("n", LazyLabel("Awesome"), Set.empty)(solved)
    )
  }

  test("should build plans for label scans with compile-time label id") {
    implicit val plan = new given {
      cost =  {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByIdSeek, _) => 2.0
        case (_: NodeByLabelScan, _) => 1.0
        case _ => Double.MaxValue
      }
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) RETURN n"

    plan.innerPlan should equal(
      NodeByLabelScan("n", lazyLabel("Awesome"), Set.empty)(solved)
    )
  }

  private val nodeIndexScanCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost] = {
    case (_: AllNodesScan, _) => 1000.0
    case (_: NodeByLabelScan, _) => 50.0
    case (_: NodeIndexScan, _) => 10.0
    case (_: NodeIndexSeek, _) => 1.0
    case (Selection(_, plan), input) => nodeIndexScanCost((plan, input))
    case _ => Double.MaxValue
  }

  test("should build plans for index scan when there is an index on the property") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) RETURN n"

    plan.innerPlan should equal(
      NodeIndexScan(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        Set.empty)(solved)
    )
  }

  test("should build plans for index scan when there is an unique index on the property") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) RETURN n"

    plan.innerPlan should equal(
      NodeIndexScan(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        Set.empty)(solved)
    )
  }

  test("should build plans for index scan when there is both an unique index and index on the property") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) RETURN n"

    plan.innerPlan should equal(
      NodeIndexScan(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        Set.empty)(solved)
    )
  }

  test("should build plans for index seek rather than index scan where there are predicates for both") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) AND n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      Selection(Seq(FunctionInvocation(FunctionName("has") _, Property(ident("n"), PropertyKeyName("prop") _) _) _),
        NodeIndexSeek(
          "n",
          LabelToken("Awesome", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)),
          SingleQueryExpression(SignedDecimalIntegerLiteral("42")_),
          Set.empty)(solved)
      )(solved))
  }

  test("should build plans for index seek when there is an index on the property") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      NodeIndexSeek(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        SingleQueryExpression(SignedDecimalIntegerLiteral("42")_),
        Set.empty)(solved)
    )
  }

  test("should build plans for unique index seek when there is an unique index on the property") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should build plans for unique index seek when there is both unique and non-unique indexes to pick") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)),
        SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should build plans for node by ID mixed with label scan when node by ID is cheaper") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n").innerPlan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_))_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for node by ID when the predicate is IN and rhs is a param") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) IN {param} RETURN n").innerPlan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Parameter("param")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for directed rel by ID when the predicate is IN and rhs is a param") {
    (new given {
    } planFor "MATCH (a)-[r]->(b) WHERE id(r) IN {param} RETURN a, r, b").innerPlan should equal (
      DirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param")_), "a", "b", Set.empty)(solved)
    )
  }

  test("should build plans for undirected rel by ID when the predicate is IN and rhs is a param") {
    (new given {
    } planFor "MATCH (a)-[r]-(b) WHERE id(r) IN {param} RETURN a, r, b").innerPlan should equal (
      UndirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param")_), "a", "b", Set.empty)(solved)
    )
  }

  test("should build plans for node by ID when the predicate is IN") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n").innerPlan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("64")_))_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for index seek when there is an index on the property and an IN predicate") {
    (new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n").innerPlan should beLike {
      case NodeIndexSeek(
              IdName("n"),
              LabelToken("Awesome", _),
              PropertyKeyToken("prop", _),
              SingleQueryExpression(SignedDecimalIntegerLiteral("42")), _) => ()
    }
  }

  test("should not use indexes for large collections") {
    // Index selectivity is 0.02, and label selectivity is 0.2.
    // The OR query will use the formula A ∪ B = ¬ ( ¬ A ∩ ¬ B ) to calculate the selectivities,
    // which gives us the formula:
    // 1 - (1 - 0.02)^15 = 1 - .98^10 = 1 - .713702596 = 0.286297404 which is less selective than .2, so the index
    // should not win
    (new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15] RETURN n").innerPlan should beLike {
      case _: Selection => ()
    }
  }

  test("should use indexes for large collections if it is a unique index") {
    val result = new given {
      cost =  {
        case (_: AllNodesScan, _)    => 10000.0
        case (_: NodeByLabelScan, _) =>  1000.0
        case (_: NodeByIdSeek, _)    =>     2.0
        case _                       => Double.MaxValue
      }
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5] RETURN n"

    result.innerPlan should beLike {
      case _: NodeUniqueIndexSeek => ()
    }
  }

  test("should build plans for label scans when a hint is given") {
    implicit val plan = new given planFor "MATCH (n:Foo:Bar:Baz) USING SCAN n:Bar RETURN n"

    plan.innerPlan should equal(
      Selection(
        Seq(HasLabels(ident("n"), Seq(LabelName("Foo")_))_, HasLabels(ident("n"), Seq(LabelName("Baz")_))_),
        NodeByLabelScan("n", LazyLabel("Bar"), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for index seek when there is an index on the property and a hint is given") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should build plans for index seek when there is an index on the property and a hint is given when returning *") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN *"

    plan.innerPlan should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should build plans for index seek when there are multiple indices on properties and a hint is given") {
    implicit val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n "

    plan.innerPlan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for unique index seek when there is an unique index on the property and a hint is given") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan.innerPlan should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should build plans for unique index seek when there are multiple unique indices on properties and a hint is given") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n"

    plan.innerPlan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans for unique index seek using IN when there are multiple unique indices on properties and a hint is given") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 IN [3] RETURN n"

    plan.innerPlan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans with NodeByIdSeek when providing id collection via identifier") {
    implicit val plan = new given {
      cost =  {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByIdSeek, _) => 2.0
        case (_: NodeByLabelScan, _) => 1.0
        case _ => Double.MaxValue
      }
    } planFor "WITH [0,1,3] AS arr MATCH (n) WHERE id(n) IN arr return count(*)"

    plan.plan should equal(
      Aggregation(
      Projection(
        Apply(
          Projection(SingleRow()(solved),Map("arr" -> Collection(List(SignedDecimalIntegerLiteral("0")_, SignedDecimalIntegerLiteral("1")_, SignedDecimalIntegerLiteral("3")_))_))(solved),
          NodeByIdSeek(IdName("n"), ManySeekableArgs(Identifier("arr")_),Set(IdName("arr")))(solved)
        )(solved),
        Map())(solved), Map(), Map("count(*)" -> CountStar()_)
      )(solved)
    )
  }
}
