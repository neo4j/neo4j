/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.{InequalitySeekRangeWrapper, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, RangeQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{ExclusiveBound, InclusiveBound, LabelId, PropertyKeyId}

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan index seek by prefix for simple prefix search based on STARTS WITH with %") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' RETURN a").plan should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
        RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(StringLiteral("prefix")_)) _),
        Set.empty)(solved)
    )
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name STARTS WITH 'short' AND a.lastname STARTS WITH 'longer' RETURN a")
      .plan should equal(
      Selection(Seq(StartsWith(Property(Identifier("a") _, PropertyKeyName("name") _) _, StringLiteral("short") _) _),
        NodeIndexSeek(
          "a",
          LabelToken("Person", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("lastname") _, PropertyKeyId(1)),
          RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(StringLiteral("longer") _)) _),
          Set.empty)(solved)
      )(solved))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix even with predicates reversed") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.lastname STARTS WITH 'longer' AND a.name STARTS WITH 'short' RETURN a")
      .plan should equal(
      Selection(Seq(StartsWith(Property(Identifier("a") _, PropertyKeyName("name") _) _, StringLiteral("short") _) _),
        NodeIndexSeek(
          "a",
          LabelToken("Person", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("lastname") _, PropertyKeyId(1)),
          RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(StringLiteral("longer") _)) _),
          Set.empty)(solved)
      )(solved))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHs combined with AND NOT") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name STARTS WITH 'longer' AND NOT a.lastname STARTS WITH 'short' RETURN a")
      .plan should equal(
      Selection(Seq(Not(StartsWith(Property(Identifier("a") _, PropertyKeyName("lastname") _) _, StringLiteral("short") _) _) _),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(StringLiteral("longer") _)) _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should plan property equality index seek instead of index seek by prefix") {
    val startsWith: StartsWith = StartsWith(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                                            StringLiteral("prefix") _) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' AND a.name = 'prefix1' RETURN a").plan should equal(
      Selection(Seq(startsWith),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  SingleQueryExpression(StringLiteral("prefix1") _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should plan property equality index seek using IN instead of index seek by prefix") {
    val startsWith: StartsWith = StartsWith(Property(Identifier("a") _, PropertyKeyName("name") _) _,
                                            StringLiteral("prefix%") _) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix%' AND a.name in ['prefix1', 'prefix2'] RETURN a").plan should equal(
      Selection(Seq(startsWith),
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
                  ManyQueryExpression(Collection(List(StringLiteral("prefix1") _, StringLiteral("prefix2") _)) _),
                  Set.empty)(solved)
      )(solved))
  }

  test("should plan index seek by numeric range for numeric inequality predicate") {
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.age < 12 RETURN a").plan should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("age") _, PropertyKeyId(0)),
        RangeQueryExpression(InequalitySeekRangeWrapper(
          RangeLessThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("12")_)))
        )_),
        Set.empty)(solved)
    )
  }

  test("should plan index seek by numeric range for numeric chained operator") {
    val than: RangeGreaterThan[SignedDecimalIntegerLiteral] =  RangeGreaterThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("6") _)))
    val than1: RangeLessThan[SignedDecimalIntegerLiteral] = RangeLessThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("12") _)))
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE 6 < a.age < 12 RETURN a").plan should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("age") _, PropertyKeyId(0)),
        RangeQueryExpression(
          InequalitySeekRangeWrapper(
            RangeBetween(
               than,
               than1
            )
          )(pos)
        ),
        Set.empty)(solved)
    )
  }

  test("should plan index seek for multiple inequality predicates and prefer the index seek with the lower cost per row") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "age")
      cost = {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByLabelScan, _) => 50.0
        case (_: NodeIndexScan, _) => 10.0
        case (plan: NodeIndexSeek, _) if plan.label.name == "name" => 1.0
        case (plan: NodeIndexSeek, _) if plan.label.name == "age" => 5.0
        case (Selection(_, plan), input) => 30.0
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a:Person) WHERE a.age > 40 AND a.name >= 'Cinderella' RETURN a").plan should equal(
      Selection(
        Seq(
          AndedPropertyInequalities(
            ident("a"),
            Property(ident("a"), PropertyKeyName("age")_)_,
            NonEmptyList(GreaterThan(Property(ident("a"), PropertyKeyName("age")_)_, SignedDecimalIntegerLiteral("40")_)_)
        )),
        NodeIndexSeek(
          "a",
          LabelToken("Person", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
          RangeQueryExpression(InequalitySeekRangeWrapper(
            RangeGreaterThan(NonEmptyList(InclusiveBound(StringLiteral("Cinderella")_)))
          )_),
          Set.empty)(solved)
      )(solved)
    )
  }
  test("should plan index seek by string range for textual inequality predicate") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } planFor "MATCH (a:Person) WHERE a.name >= 'Frodo' RETURN a").plan should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)),
        RangeQueryExpression(InequalitySeekRangeWrapper(
          RangeGreaterThan(NonEmptyList(InclusiveBound(StringLiteral("Frodo")_)))
        )_),
        Set.empty)(solved)
    )
  }

  test("should plan all nodes scans") {
    (new given {
    } planFor "MATCH (n) RETURN n").plan should equal(
      AllNodesScan("n", Set.empty)(solved)
    )
  }

  test("should plan label scans even without having a compile-time label id") {
    (new given {
      cost =  {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByIdSeek, _) => 2.0
        case (_: NodeByLabelScan, _) => 1.0
        case _ => Double.MaxValue
      }
    } planFor "MATCH (n:Awesome) RETURN n").plan should equal(
      NodeByLabelScan("n", LazyLabel("Awesome"), Set.empty)(solved)
    )
  }

  test("should plan label scans when having a compile-time label id") {
    implicit val plan = new given {
      cost =  {
        case (_: AllNodesScan, _) => 1000.0
        case (_: NodeByIdSeek, _) => 2.0
        case (_: NodeByLabelScan, _) => 1.0
        case _ => Double.MaxValue
      }
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) RETURN n"

    plan.plan should equal(
      NodeByLabelScan("n", lazyLabel("Awesome"), Set.empty)(solved)
    )
  }

  private val nodeIndexScanCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost] = {
    case (_: AllNodesScan, _) => 1000.0
    case (_: NodeByLabelScan, _) => 50.0
    case (_: NodeIndexScan, _) => 10.0
    case (nodeIndexSeek: NodeIndexSeek, _) =>
      val planCardinality = nodeIndexSeek.solved.estimatedCardinality.amount
      val rowCost = 1.0
      val allNodesCardinality = 1000.0
      rowCost * planCardinality / allNodesCardinality
    case (Selection(_, plan), input) => nodeIndexScanCost((plan, input))
    case _ => Double.MaxValue
  }

  test("should plan index scan for has(n.prop)") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) RETURN n"

    plan.plan should equal(
      NodeIndexScan(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        Set.empty)(solved)
    )
  }

  test("should plan unique index scan for has(n.prop)") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) RETURN n"

    plan.plan should equal(
      NodeIndexScan(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        Set.empty)(solved)
    )
  }

  test("should plan index seek instead of index scan when there are predicates for both") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } planFor "MATCH (n:Awesome) WHERE has(n.prop) AND n.prop = 42 RETURN n"

    plan.plan should equal(
      Selection(Seq(FunctionInvocation(FunctionName("exists") _, Property(ident("n"), PropertyKeyName("prop") _) _) _),
        NodeIndexSeek(
          "n",
          LabelToken("Awesome", LabelId(0)),
          PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)),
          SingleQueryExpression(SignedDecimalIntegerLiteral("42")_),
          Set.empty)(solved)
      )(solved))
  }

  test("should plan index seek when there is an index on the property") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexSeek(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken(PropertyKeyName("prop")_, PropertyKeyId(0)),
        SingleQueryExpression(SignedDecimalIntegerLiteral("42")_),
        Set.empty)(solved)
    )
  }

  test("should plan unique index seek when there is an unique index on the property") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should plan node by ID lookup instead of label scan when the node by ID lookup is cheaper") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n").plan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_))_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan node by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) IN {param} RETURN n").plan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Parameter("param")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan directed rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } planFor "MATCH (a)-[r]->(b) WHERE id(r) IN {param} RETURN a, r, b").plan should equal (
      DirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param")_), "a", "b", Set.empty)(solved)
    )
  }

  test("should plan undirected rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } planFor "MATCH (a)-[r]-(b) WHERE id(r) IN {param} RETURN a, r, b").plan should equal (
      UndirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param")_), "a", "b", Set.empty)(solved)
    )
  }

  test("should plan node by ID lookup based on an IN predicate") {
    (new given {
      knownLabels = Set("Awesome")
    } planFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n").plan should equal (
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("64")_))_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan index seek when there is an index on the property and an IN predicate") {
    (new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n").plan should beLike {
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
    } planFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15] RETURN n").plan should beLike {
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

    result.plan should beLike {
      case _: NodeUniqueIndexSeek => ()
    }
  }

  test("should plan hinted label scans") {

    implicit val plan = new given {
      cost = {
        case (_: Selection, _) => 20.0
        case (_: NodeHashJoin, _) => 1000.0
        case (_: NodeByLabelScan, _) => 20.0
      }
    } planFor "MATCH (n:Foo:Bar:Baz) USING SCAN n:Bar RETURN n"

    plan.plan should equal(
      Selection(
        Seq(HasLabels(ident("n"), Seq(LabelName("Foo")_))_, HasLabels(ident("n"), Seq(LabelName("Baz")_))_),
        NodeByLabelScan("n", LazyLabel("Bar"), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan hinted index seek") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should plan hinted index seek when returning *") {
    implicit val plan = new given {
      indexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN *"

    plan.plan should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should plan hinted index seek when there are multiple indices") {
    implicit val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n "

    plan.plan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan hinted unique index seek") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan.plan should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop", PropertyKeyId(0)), SingleQueryExpression(SignedDecimalIntegerLiteral("42")_), Set.empty)(solved)
    )
  }

  test("should plan hinted unique index seek when there are multiple unique indices") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n"

    plan.plan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan hinted unique index seek based on an IN predicate  when there are multiple unique indices") {
    implicit val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } planFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 IN [3] RETURN n"

    plan.plan should equal(
      Selection(
        List(In(Property(ident("n"), PropertyKeyName("prop1")_)_, Collection(Seq(SignedDecimalIntegerLiteral("42")_))_)_),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), PropertyKeyToken("prop2", PropertyKeyId(1)), SingleQueryExpression(SignedDecimalIntegerLiteral("3")_), Set.empty)(solved)
      )(solved)
    )
  }

  test("should plan node by ID seek based on a predicate with an id collection identifier as the rhs") {
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
        Apply(
          Projection(SingleRow()(solved),Map("arr" -> Collection(List(SignedDecimalIntegerLiteral("0")_, SignedDecimalIntegerLiteral("1")_, SignedDecimalIntegerLiteral("3")_))_))(solved),
          NodeByIdSeek(IdName("n"), ManySeekableArgs(Identifier("arr")_),Set(IdName("arr")))(solved)
        )(solved),
        Map(), Map("count(*)" -> CountStar()_)
      )(solved)
    )
  }

  test("should use index on label and property") {
    val plan = new given {
      indexOn("Crew", "name")
    } planFor "MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n"

    plan shouldBe using[NodeIndexSeek]
  }

  test("should use index when there are multiple labels on the node") {
    val plan = new given {
      indexOn("Crew", "name")
      cost = {
        case (_: NodeByIdSeek, _) => 1.0
        case _ => 100.0
      }
    } planFor "MATCH (n:Matrix:Crew) WHERE n.name = 'Neo' RETURN n"

    plan shouldBe using[NodeIndexSeek]
  }
}
