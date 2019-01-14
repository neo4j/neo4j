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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_5.planner.StubbedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.v3_5.RegularPlannerQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  test("should plan index seek by prefix for simple prefix search based on STARTS WITH with prefix") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' RETURN a")._2 should equal(
      IndexSeek("a:Person(name STARTS WITH 'prefix')")
    )
  }

  test("should prefer cheaper optional expand over joins, even if not cheaper before rewriting") {
    (new given {
      cost = {
        case (_: RightOuterHashJoin, _, _) => 6.610321376825E9
        case (_: LeftOuterHashJoin, _, _) => 8.1523761738E9
        case (_: Apply, _, _) => 7.444573003149691E9
        case (_: OptionalExpand, _, _) => 4.76310362E8
        case (_: Optional, _, _) => 7.206417822149691E9
        case (_: Selection, _, _) => 1.02731056E8
        case (_: Expand, _, _) => 7.89155379E7
        case (_: AllNodesScan, _, _) => 3.50735724E7
        case (_: Argument, _, _) => 2.38155181E8
        case (_: ProjectEndpoints, _, _) => 11.0
      }
    } getLogicalPlanFor
      """UNWIND {createdRelationships} as r
        |MATCH (source)-[r]->(target)
        |WITH source AS p
        |OPTIONAL MATCH (p)<-[follow]-() WHERE type(follow) STARTS WITH 'ProfileFavorites'
        |WITH p, count(follow) as fc
        |RETURN 1
      """.stripMargin)._2 should beLike {
      case Projection(Aggregation(_: OptionalExpand, _, _), _) => ()
    }
  }

  test("should plan index seek by prefix for simple prefix search based on CONTAINS substring") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name CONTAINS 'substring' RETURN a")._2 should equal(
      IndexSeek("a:Person(name CONTAINS 'substring')")
    )
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'short' AND a.lastname STARTS WITH 'longer' RETURN a")
      ._2 should equal(
        Selection(Ands(Set(StartsWith(Property(Variable("a") _, PropertyKeyName("name") _) _, StringLiteral("short") _) _))_,
          IndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Map("lastname" -> 1))
        ))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix even with predicates reversed") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.lastname STARTS WITH 'longer' AND a.name STARTS WITH 'short' RETURN a")
      ._2 should equal(
      Selection(Ands(Set(StartsWith(Property(Variable("a") _, PropertyKeyName("name") _) _, StringLiteral("short") _) _))_,
        IndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Map("lastname" -> 1))
      ))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHs combined with AND NOT") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'longer' AND NOT a.lastname STARTS WITH 'short' RETURN a")
      ._2 should equal(
      Selection(Ands(Set(Not(StartsWith(Property(Variable("a") _, PropertyKeyName("lastname") _) _, StringLiteral("short") _) _) _))_,
                IndexSeek("a:Person(name STARTS WITH 'longer')")
      ))
  }

  test("should plan property equality index seek instead of index seek by prefix") {
    val startsWith: StartsWith = StartsWith(cached("a.name"),
                                            StringLiteral("prefix") _) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' AND a.name = 'prefix1' RETURN a")._2 should equal(
      Selection(Ands(Set(startsWith)) _,
                IndexSeek("a:Person(name = 'prefix1')", GetValue)
      ))
  }

  test("should plan property equality index seek using IN instead of index seek by prefix") {
    val startsWith: StartsWith = StartsWith(cached("a.name"),
                                            StringLiteral("prefix%") _) _
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix%' AND a.name in ['prefix1', 'prefix2'] RETURN a")._2 should equal(
      Selection(Ands(Set(startsWith)) _,
                NodeIndexSeek(
                  "a",
                  LabelToken("Person", LabelId(0)),
                  Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("name") _, PropertyKeyId(0)), GetValue)),
                  ManyQueryExpression(ListLiteral(List(StringLiteral("prefix1") _, StringLiteral("prefix2") _)) _),
                  Set.empty,
                  IndexOrderNone)
      ))
  }

  test("should plan index seek by numeric range for numeric inequality predicate") {
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age < 12 RETURN a")._2 should equal(
      IndexSeek("a:Person(age < 12)")
    )
  }

  test("should plan index seek by numeric range for numeric chained operator") {
    val than: RangeGreaterThan[SignedDecimalIntegerLiteral] =  RangeGreaterThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("6") _)))
    val than1: RangeLessThan[SignedDecimalIntegerLiteral] = RangeLessThan(NonEmptyList(ExclusiveBound(SignedDecimalIntegerLiteral("12") _)))
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE 6 < a.age < 12 RETURN a")._2 should equal(
      NodeIndexSeek(
        "a",
        LabelToken("Person", LabelId(0)),
        Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("age") _, PropertyKeyId(0)), DoNotGetValue)),
        RangeQueryExpression(
          InequalitySeekRangeWrapper(
            RangeBetween(
               than,
               than1
            )
          )(pos)
        ),
        Set.empty,
        IndexOrderNone)
    )
  }

  test("should not plan apply that will hide a cartesian product") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome), (m:Awesome) WHERE n.prop1 < 42 AND m.prop2 < 42 AND n.prop1 = m.prop2 RETURN n")._2

    plan should beLike {
      case ValueHashJoin(
        NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _),
        NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _),
        Equals(_, _)
      ) => ()
    }
  }

  test("should plan nested index join with apply where rhs depends on lhs") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
      indexOn("Awesome", "prop3")
    } getLogicalPlanFor "MATCH (n:Awesome), (m:Awesome) WHERE n.prop1 < 42 AND m.prop2 < 42 AND n.prop3 = m.prop4 RETURN n")._2

    plan should beLike {
      case Selection(_, Apply(
        NodeIndexSeek(_,_,_,RangeQueryExpression(_),_,_),
        NodeIndexSeek(_,_,_,SingleQueryExpression(_),_,_))) => ()
    }
  }

  test("should plan index seek for multiple inequality predicates and prefer the index seek with the lower cost per row") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "age")
      cost = {
        case (_: AllNodesScan, _, _) => 1000.0
        case (_: NodeByLabelScan, _, _) => 50.0
        case (_: NodeIndexScan, _, _) => 10.0
        case (plan: NodeIndexSeek, _, _) if plan.label.name == "name" => 1.0
        case (plan: NodeIndexSeek, _, _) if plan.label.name == "age" => 5.0
        case (Selection(_, plan), input, _) => 30.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age > 40 AND a.name >= 'Cinderella' RETURN a")._2 should equal(
      Selection(
        Seq(
          AndedPropertyInequalities(
            varFor("a"),
            Property(varFor("a"), PropertyKeyName("age")_)_,
            NonEmptyList(GreaterThan(Property(varFor("a"), PropertyKeyName("age")_)_, SignedDecimalIntegerLiteral("40")_)_)
        )),
        IndexSeek("a:Person(name >= 'Cinderella')")
      )
    )
  }

  test("should plan index seek by string range for textual inequality predicate") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name >= 'Frodo' RETURN a")._2 should equal(
      IndexSeek("a:Person(name >= 'Frodo')")
    )
  }

  test("should plan all nodes scans") {
    (new given {
    } getLogicalPlanFor "MATCH (n) RETURN n")._2 should equal(
      AllNodesScan("n", Set.empty)
    )
  }

  test("should plan label scans even without having a compile-time label id") {
    (new given {
      cost =  {
        case (_: AllNodesScan, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _) => 2.0
        case (_: NodeByLabelScan, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n")._2 should equal(
      NodeByLabelScan("n", lblName("Awesome"), Set.empty)
    )
  }

  test("should plan label scans when having a compile-time label id") {
    val plan = new given {
      cost =  {
        case (_: AllNodesScan, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _) => 2.0
        case (_: NodeByLabelScan, _, _) => 1.0
        case _ => Double.MaxValue
      }
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n"

    plan._2 should equal(
      NodeByLabelScan("n", lblName("Awesome"), Set.empty)
    )
  }

  private val nodeIndexScanCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = {
    case (_: AllNodesScan, _, _) => 1000.0
    case (_: NodeByLabelScan, _, _) => 50.0
    case (_: NodeIndexScan, _, _) => 10.0
    case (_: NodeIndexContainsScan, _, _) => 10.0
    case (nodeIndexSeek: NodeIndexSeek, _, cardinalities) =>
      val planCardinality = cardinalities.get(nodeIndexSeek.id).amount
      val rowCost = 1.0
      val allNodesCardinality = 1000.0
      rowCost * planCardinality / allNodesCardinality
    case (Selection(_, plan), input, c) => nodeIndexScanCost((plan, input, c))
    case _ => Double.MaxValue
  }

  private val nodeIndexSeekCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = {
    case (_: AllNodesScan, _, _) => 1000000000.0
    case (_: NodeIndexSeek, _, _) => 0.1
    case (Expand(plan, _, _, _, _, _, _), input, c) => nodeIndexSeekCost((plan, input, c))
    case (Selection(_, plan), input, c) => nodeIndexSeekCost((plan, input, c))
    case _ => 1000.0
  }

  test("should plan index scan for exists(n.prop)") {
    val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) RETURN n"

    plan._2 should equal(IndexSeek("n:Awesome(prop)"))
  }

  test("should plan unique index scan for exists(n.prop)") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) RETURN n"

    plan._2 should equal(IndexSeek("n:Awesome(prop)"))
  }

  test("should plan index seek instead of index scan when there are predicates for both") {
    val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) AND n.prop = 42 RETURN n"

    plan._2 should equal(
      Selection(Ands(Set(FunctionInvocation(FunctionName("exists") _, cached("n.prop")) _))_,
        IndexSeek("n:Awesome(prop = 42)", GetValue)
      ))
  }

  test("should plan index seek when there is an index on the property") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._2 should equal(
      IndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan unique index seek when there is an unique index on the property") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._2 should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue)), SingleQueryExpression(SignedDecimalIntegerLiteral("42") _), Set.empty, IndexOrderNone)
    )
  }

  test("should plan node by ID lookup instead of label scan when the node by ID lookup is cheaper") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n")._2 should equal (
      Selection(
        Ands(Set(HasLabels(Variable("n")_, Seq(LabelName("Awesome")_))_)) _,
        NodeByIdSeek("n", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_), Set.empty)
      )
    )
  }

  test("should plan node by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN {param} RETURN n")._2 should equal (
      Selection(
        Ands(Set(HasLabels(Variable("n")_, Seq(LabelName("Awesome")_))_))_,
        NodeByIdSeek("n", ManySeekableArgs(Parameter("param", CTAny)_), Set.empty)
      )
    )
  }

  test("should plan directed rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } getLogicalPlanFor "MATCH (a)-[r]->(b) WHERE id(r) IN {param} RETURN a, r, b")._2 should equal (
      DirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param", CTAny)_), "a", "b", Set.empty)
    )
  }

  test("should plan undirected rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } getLogicalPlanFor "MATCH (a)-[r]-(b) WHERE id(r) IN {param} RETURN a, r, b")._2 should equal (
      UndirectedRelationshipByIdSeek("r", ManySeekableArgs(Parameter("param", CTAny)_), "a", "b", Set.empty)
    )
  }

  test("should plan node by ID lookup based on an IN predicate") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n")._2 should equal (
      Selection(
        Ands(Set(HasLabels(Variable("n")_, Seq(LabelName("Awesome")_))_))_,
        NodeByIdSeek("n", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("64")_))_), Set.empty)
      )
    )
  }

  test("should plan index seek when there is an index on the property and an IN predicate") {
    (new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n")._2 should beLike {
      case NodeIndexSeek(
              "n",
              LabelToken("Awesome", _),
      Seq(IndexedProperty(PropertyKeyToken("prop", _), DoNotGetValue)),
              SingleQueryExpression(SignedDecimalIntegerLiteral("42")), _, _) => ()
    }
  }

  test("should use indexes for large collections if it is a unique index") {
    val result = new given {
      cost =  {
        case (_: AllNodesScan, _, _)    => 10000.0
        case (_: NodeByLabelScan, _, _) =>  1000.0
        case (_: NodeByIdSeek, _, _)    =>     2.0
        case _                       => Double.MaxValue
      }
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5] RETURN n"

    result._2 should beLike {
      case _: NodeUniqueIndexSeek => ()
    }
  }

  //
  // Composite indexes
  // WHERE n:Label AND (n.prop = $val1 OR (n.prop = $val2 AND n.bar = $val3))

  test("should plan composite index seek when there is an index on two properties and both are in equality predicates") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.prop2 = 'foo' RETURN n"

    plan._2 should equal(
      IndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
    )
  }

  test("should plan composite index seek when there is an index on two properties and both are in equality predicates regardless of predicate order") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND n.prop = 42 RETURN n"

    plan._2 should equal(
      IndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
    )
  }

  test("should plan composite index seek and filter when there is an index on two properties and both are in equality predicates together with other predicates") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND exists(n.name) AND n.prop = 42 RETURN n"

    plan._2 should equal(
      Selection(Ands(Set(FunctionInvocation(FunctionName("exists") _, Property(varFor("n"), PropertyKeyName("name") _) _) _))_,
        IndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
      )
    )
  }

  //
  // index hints
  //

  test("should plan hinted label scans") {

    val plan = new given {
      cost = {
        case (_: Selection, _, _) => 20.0
        case (_: NodeHashJoin, _, _) => 1000.0
        case (_: NodeByLabelScan, _, _) => 20.0
      }
    } getLogicalPlanFor "MATCH (n:Foo:Bar:Baz) USING SCAN n:Bar RETURN n"

    plan._2 should equal(
      Selection(
        Ands(Set(HasLabels(varFor("n"), Seq(LabelName("Foo")_))_, HasLabels(varFor("n"), Seq(LabelName("Baz")_))_))_,
        NodeByLabelScan("n", lblName("Bar"), Set.empty)
      )
    )
  }

  test("should plan hinted index seek") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan._2 should equal(
      IndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan hinted index seek when returning *") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN *"

    plan._2 should equal(
      IndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan hinted index seek with or") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) USING INDEX n:Awesome(prop) WHERE n.prop = 42 OR n.prop = 1337 RETURN n"

    plan._2 should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue)), ManyQueryExpression(ListLiteral(List(SignedDecimalIntegerLiteral("42") _, SignedDecimalIntegerLiteral("1337") _)) _), Set.empty, IndexOrderNone)
    )
  }

  test("should plan hinted index seek when there are multiple indices") {
    val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n "

    plan._2 should equal(
      Selection(
        Ands(Set(In(Property(varFor("n"), PropertyKeyName("prop1")_)_, ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_)_))_,
        IndexSeek("n:Awesome(prop2 = 3)", propIds = Map("prop2" -> 1))
      )
    )
  }

  test("should plan hinted index seek when there are multiple or indices") {
    val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND (n.prop1 = 42 OR n.prop2 = 3) RETURN n "

    val seek1 = IndexSeek("n:Awesome(prop1 = 42)", CanGetValue)
    val seek2 = IndexSeek("n:Awesome(prop2 = 3)", CanGetValue, propIds = Map("prop2" -> 1))
    val union: Union = Union(seek2, seek1)
    val distinct = Distinct(union, Map("n" -> varFor("n")))

    plan._2 should equal(distinct)
  }

  test("should plan hinted unique index seek") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan._2 should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue)), SingleQueryExpression(SignedDecimalIntegerLiteral("42") _), Set.empty, IndexOrderNone)
    )
  }

  test("should plan hinted unique index seek when there are multiple unique indices") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n"

    plan._2 should equal(
      Selection(
        Ands(Set(In(Property(varFor("n"), PropertyKeyName("prop1")_)_, ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_)_))_,
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("prop2", PropertyKeyId(1)), DoNotGetValue)), SingleQueryExpression(SignedDecimalIntegerLiteral("3") _), Set.empty, IndexOrderNone)
      )
    )
  }

  test("should plan hinted unique index seek based on an IN predicate  when there are multiple unique indices") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 IN [3] RETURN n"

    plan._2 should equal(
      Selection(
        Ands(Set(In(Property(varFor("n"), PropertyKeyName("prop1")_)_, ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_)_))_,
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("prop2", PropertyKeyId(1)), DoNotGetValue)), SingleQueryExpression(SignedDecimalIntegerLiteral("3") _), Set.empty, IndexOrderNone)
      )
    )
  }

  test("should plan node by ID seek based on a predicate with an id collection variable as the rhs") {
    val plan = new given {
      cost =  {
        case (_: AllNodesScan, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _) => 2.0
        case (_: NodeByLabelScan, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "WITH [0,1,3] AS arr MATCH (n) WHERE id(n) IN arr return count(*)"

    plan._2 should equal(
      Aggregation(
        Apply(
          Projection(Argument(),Map("arr" -> ListLiteral(List(SignedDecimalIntegerLiteral("0")_, SignedDecimalIntegerLiteral("1")_, SignedDecimalIntegerLiteral("3")_))_)),
          NodeByIdSeek("n", ManySeekableArgs(Variable("arr")_),Set("arr"))
        ),
        Map(), Map("count(*)" -> CountStar()_)
      )
    )
  }

  test("should use index on label and property") {
    val plan = (new given {
      indexOn("Crew", "name")
    } getLogicalPlanFor "MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n")._2

    plan shouldBe using[NodeIndexSeek]
  }

  test("should use index when there are multiple labels on the node") {
    val plan = (new given {
      indexOn("Crew", "name")
      indexOn("Matrix", "someOther")
    } getLogicalPlanFor "MATCH (n:Matrix:Crew) WHERE n.name = 'Neo' RETURN n")._2

    plan shouldBe using[NodeIndexSeek]
  }

  test("should be able to OR together two index seeks") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 'apa' RETURN n")._2

    val seek1 = IndexSeek("n:Awesome(prop1 = 42)", CanGetValue)
    val seek2 = IndexSeek("n:Awesome(prop2 = 'apa')", CanGetValue, propIds = Map("prop2" -> 1))
    val union: Union = Union(seek2, seek1)
    val distinct = Distinct(union, Map("n" -> varFor("n")))

    plan should equal(distinct)
  }

  test("should be able to OR together two label scans") {
    val x = (new given {
      knownLabels = Set("X", "Y")
    } getLogicalPlanFor "MATCH (n) WHERE n:X OR n:Y RETURN n")._2

    x should beLike {
      case Distinct(
        Union(
          NodeByLabelScan(
            "n",
            LabelName("X"), _),
          NodeByLabelScan(
            "n",
            LabelName("Y"), _)),
      _)
      => ()
    }
  }

  test("should be able to OR together two index range seeks") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 >= 42 OR n.prop2 STARTS WITH 'apa' RETURN n")._2

    val prop1Predicate = GreaterThanOrEqual(prop("n", "prop1"), literalInt(42))(pos)
    val seek1 = Selection(Seq(prop1Predicate), IndexSeek("n:Awesome(prop1)"))
    val seek2 = IndexSeek("n:Awesome(prop2 STARTS WITH 'apa')", propIds = Map("prop2" -> 1))
    val union = Union(seek1, seek2)
    val distinct = Distinct(union, Map("n" -> varFor("n")))

    plan should equal(distinct)
  }

  test("should be able to OR together two index seeks with different labels") {
    val plan = (new given {
      indexOn("Label1", "prop1")
      indexOn("Label2", "prop2")
    } getLogicalPlanFor "MATCH (n:Label1:Label2) WHERE n.prop1 = 'val' OR n.prop2 = 'val' RETURN n")._2

    val labelPredicate1 = HasLabels(Variable("n")(pos), Seq(LabelName("Label1")(pos)))(pos)
    val labelPredicate2 = HasLabels(Variable("n")(pos), Seq(LabelName("Label2")(pos)))(pos)

    val seek1 = IndexSeek("n:Label1(prop1 = 'val')", CanGetValue)
    val seek2 = IndexSeek("n:Label2(prop2 = 'val')", CanGetValue, propIds = Map("prop2" -> 1), labelId = 1)

    val union: Union = Union(seek2, seek1)
    val distinct = Distinct(union, Map("n" -> varFor("n")))
    val filter = Selection(Seq(labelPredicate1, labelPredicate2), distinct)

    plan should equal(filter)
  }

  test("should be able to OR together four index seeks") {
    val plan = (new given {
      indexOn("Label1", "prop1")
      indexOn("Label1", "prop2")
      indexOn("Label2", "prop1")
      indexOn("Label2", "prop2")
    } getLogicalPlanFor "MATCH (n:Label1:Label2) WHERE n.prop1 = 'val' OR n.prop2 = 'val' RETURN n")._2

    val labelPredicate1 = HasLabels(Variable("n")(pos), Seq(LabelName("Label1")(pos)))(pos)
    val labelPredicate2 = HasLabels(Variable("n")(pos), Seq(LabelName("Label2")(pos)))(pos)

    val seek1 = IndexSeek("n:Label1(prop1 = 'val')", CanGetValue)
    val seek2 = IndexSeek("n:Label1(prop2 = 'val')", CanGetValue, propIds = Map("prop2" -> 1))
    val seek3 = IndexSeek("n:Label2(prop1 = 'val')", CanGetValue, labelId = 1)
    val seek4 = IndexSeek("n:Label2(prop2 = 'val')", CanGetValue, labelId = 1, propIds = Map("prop2" -> 1))

    val union: Union = Union( Union( Union(seek2, seek4), seek1), seek3)
    val distinct = Distinct(union, Map("n" -> varFor("n")))
    val filter = Selection(Seq(labelPredicate1, labelPredicate2), distinct)

    plan should equal(filter)
  }

  test("should use transitive closure to figure out we can use index") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE a.name = b.prop AND b.prop = 42 RETURN b")._2 should beLike {
      case Selection(_, Expand(NodeIndexSeek("a", _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  test("should use transitive closure to figure out emergent equalities") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE b.prop = a.name AND b.prop = 42 RETURN b")._2 should beLike {
      case Selection(_, Expand(NodeIndexSeek("a", _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  //---------------------------------------------------------------------------
  // Test expand order with multiple configurations and
  // unsupported.cypher.plan_with_minimum_cardinality_estimates setting
  //
  // To succeed this test assumes:
  // *  (:A) should have lower cardinality than (:B) and (:C) so it is selected as starting point
  // * (a)--(b) should have lower cardinality than (a)--(c) so that it is expanded first
  //
  // Ideally (and at the time of writing) the intrinsic order when the cardinalities are equal
  // is different from the assertion and would cause failure
  private def testAndAssertExpandOrder(config: StubbedLogicalPlanningConfiguration) {
    val query = "MATCH (b:B)-[rB]->(a:A)<-[rC]-(c:C) RETURN a, b, c"

    val plan = (config getLogicalPlanFor query)._2

    // Expected plan
    // Since (a)--(b) has a lower cardinality estimate than (a)--(c) it should be selected first
    val scanA = NodeByLabelScan("a", LabelName("A")(pos), Set.empty)
    val expandB = Expand(scanA, "a", INCOMING, Seq.empty, "b", "rB", ExpandAll)
    val selectionB = Selection(Seq(HasLabels(Variable("b")(pos), Seq(LabelName("B")(pos)))(pos)), expandB)
    val expandC = Expand(selectionB, "a", INCOMING, Seq.empty, "c", "rC", ExpandAll)
    val selectionC = Selection(Seq(Not(Equals(Variable("rB")(pos), Variable("rC")(pos))(pos))(pos),
      HasLabels(Variable("c")(pos), Seq(LabelName("C")(pos)))(pos)), expandC)
    val expected = selectionC

    plan should equal(expected)
  }

  test("should pick expands in an order that minimizes early cardinality increase") {
    val config = new given {
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 200.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 300.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase (plan_with_minimum_cardinality_estimates enabled)") {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 200.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 300.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase with estimates < 1.0") {
    val config = new given {
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") =>  5.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 10.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 0.4
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 0.5
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase with estimates < 1.0 (plan_with_minimum_cardinality_estimates enabled)") {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") =>  5.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 10.0
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 0.4
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 0.5
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }
}
