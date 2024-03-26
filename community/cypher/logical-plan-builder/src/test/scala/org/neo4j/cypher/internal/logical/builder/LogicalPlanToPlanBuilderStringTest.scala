/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.delete
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.graphdb.schema.IndexType

import java.lang.reflect.Modifier
import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

class LogicalPlanToPlanBuilderStringTest extends CypherFunSuite with TestName {

  private val testedOperators = mutable.Set[String]()

  testPlan("letSelectOrSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .letSelectOrSemiApply("idName", "false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("letSelectOrAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("produceResults",
    new TestPlanBuilder()
      .produceResults("x")
      .argument("x")
      .build())

  testPlan("produceResults of property",
    new TestPlanBuilder()
      .produceResults("`x.prop`")
      .allNodeScan("x")
      .build())

  testPlan("argument",
    new TestPlanBuilder()
      .produceResults("x")
      .argument("x", "y")
      .build())

  testPlan("input",
    new TestPlanBuilder()
      .produceResults("x")
      .input(Seq("n", "m"), Seq("r", "q", "p"), Seq("v1"), nullable = false)
      .build())

  testPlan("allNodeScan",
    new TestPlanBuilder()
      .produceResults("x")
      .allNodeScan("x")
      .build())

  testPlan("nodeByLabelScan",
    new TestPlanBuilder()
      .produceResults("x")
      .nodeByLabelScan("x", "X")
      .build())

  testPlan("nodeByLabelScan full",
    new TestPlanBuilder()
      .produceResults("x")
      .nodeByLabelScan("x", "X", IndexOrderDescending, "foo")
      .build())

  testPlan("expandAll",
    new TestPlanBuilder()
      .produceResults("x")
      .expandAll("(x)-[r]->(y)")
      .expandAll("(x)-[r]->(y)")
      .expandAll("(x)<-[r]-(y)")
      .expandAll("(x)-[r]-(y)")
      .expandAll("(x)-[r:R]-(y)")
      .expandAll("(x)-[r:R|L]-(y)")
      .argument()
      .build())

  // Var expand
  testPlan("expand",
    new TestPlanBuilder()
      .produceResults("x")
      .expand("(x)-[r*0..0]->(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)<-[r*0..1]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r*2..5]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r:REL*1..2]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r:REL|LER*1..2]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r*1..2]-(y)", expandMode = ExpandInto, projectedDir = OUTGOING)
      .expand("(x)-[r*1..2]->(y)", expandMode = ExpandAll, projectedDir = INCOMING)
      .expand("(x)-[r*1..2]->(y)", expandMode = ExpandAll, projectedDir = BOTH)
      .expand("(x)-[r*1..2]->(y)", expandMode = ExpandAll, projectedDir = BOTH, nodePredicate = Predicate("n", "id(n) <> 5"))
      .expand("(x)-[r*1..3]->(y)", expandMode = ExpandAll, projectedDir = BOTH, relationshipPredicate = Predicate("r", "id(r) <> 5"))
      .argument()
      .build())

  testPlan("shortestPath",
    new TestPlanBuilder()
      .produceResults("x")
      .shortestPath("(x)<-[r]-(y)")
      .shortestPath("(x)-[r*0..0]->(y)", pathName = Some("path"))
      .shortestPath("(x)<-[r*0..1]-(y)")
      .shortestPath("(x)-[r*2..5]-(y)", all = true)
      .shortestPath("(x)-[r:REL*1..2]-(y)", predicates = Seq("all(n IN nodes(path) WHERE id(n) <> 5)"))
      .shortestPath("(x)-[r:REL|LER*1..2]-(y)")
      .shortestPath("(x)-[r*1..2]-(y)")
      .shortestPath("(x)-[r*1..2]->(y)", pathName = Some("path"), all = true, predicates = Seq("all(n IN nodes(path) WHERE id(n) <> 5)", "all(rel IN relationships(path) WHERE id(rel) <> 7)"))
      .shortestPath("(x)-[r*1..2]->(y)", disallowSameNode = false)
      .shortestPath("(x)-[r*1..3]->(y)", withFallback = true)
      .argument()
      .build())

  testPlan("pruningVarExpand",
    new TestPlanBuilder()
      .produceResults("x")
      .pruningVarExpand("(x)-[*0..0]->(y)")
      .pruningVarExpand("(x)<-[*0..1]-(y)")
      .pruningVarExpand("(x)-[*2..5]-(y)")
      .pruningVarExpand("(x)-[:REL*1..2]-(y)")
      .pruningVarExpand("(x)-[:REL|LER*1..2]-(y)")
      .pruningVarExpand("(x)-[*1..2]-(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)", nodePredicate = Predicate("n", "id(n) <> 5"))
      .pruningVarExpand("(x)-[*1..3]->(y)", relationshipPredicate = Predicate("r", "id(r) <> 5"))
      .argument()
      .build())

  testPlan("expandInto",
    new TestPlanBuilder()
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandInto("(x)-[r]->(y)")
      .expandInto("(x)<-[r]-(y)")
      .expandInto("(x)-[r]-(y)")
      .expandInto("(x)-[r:REL]-(y)")
      .expandInto("(x)-[r:REL|LER]-(y)")
      .argument()
      .build())

  testPlan("optionalExpandAll",
    new TestPlanBuilder()
      .produceResults("x")
      .optionalExpandAll("(x)-[r]->(y)")
      .optionalExpandAll("(x)-[r]->(y)")
      .optionalExpandAll("(x)-[r:REL]->(y)")
      .optionalExpandAll("(x)-[r:REL|LER]->(y)")
      .optionalExpandAll("(x)-[r]->(y)", Some("y.num > 20"))
      .optionalExpandAll("(x)-[r]->(y)", Some("x:X"))
      .optionalExpandAll("(x)-[r]->(y)", Some("r:R"))
      .optionalExpandAll("(x)-[r]->(y)", Some("a:A"))
      .argument()
      .build())

  testPlan("optionalExpandInto",
    new TestPlanBuilder()
      .produceResults("x")
      .optionalExpandInto("(x)-[r]->(y)")
      .optionalExpandInto("(x)-[r:REL]->(y)")
      .optionalExpandInto("(x)-[r:REL|LER]->(y)")
      .optionalExpandInto("(x)-[r]->(y)", Some("y.num > 20"))
      .argument()
      .build())

  testPlan("limit",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .limit(5)
      .argument()
      .build())

  testPlan("exhaustiveLimit",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .exhaustiveLimit(5)
      .argument()
      .build())

  testPlan("skip",
           new TestPlanBuilder()
             .produceResults("x", "y")
             .skip(5)
             .argument()
             .build())

  testPlan("aggregation",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .aggregation(Seq("x AS x"), Seq("collect(y) AS y"))
      .argument()
      .build())

  testPlan("orderedAggregation",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedAggregation(Seq("x AS x"), Seq("collect(y) AS y"), Seq("x"))
      .orderedAggregation(Seq("x AS x", "1 + n.foo AS y"), Seq("collect(y) AS y"), Seq("x", "1 + n.foo"))
      .argument()
      .build())

  testPlan("apply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("apply with non-default argument",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply(fromSubquery = true)
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("semiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .semiApply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("antiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .antiSemiApply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("letSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "idName")
      .letSemiApply("idName")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("letAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "let")
      .letAntiSemiApply("let")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("conditionalApply",
    new TestPlanBuilder()
      .produceResults("x")
      .conditionalApply("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("antiConditionalApply",
    new TestPlanBuilder()
      .produceResults("x")
      .antiConditionalApply("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("selectOrSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .selectOrSemiApply("false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("selectOrAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .selectOrAntiSemiApply("false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("rollUpApply",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .rollUpApply("list", "y")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build())

  testPlan("foreachApply",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .foreachApply("i", "[1, 2, 3]")
      .|.setNodeProperty("n", "n.prop", "i")
      .|.create(createNode("n"))
      .|.argument("i")
      .allNodeScan("x")
      .build())

  testPlan("foreach",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .foreach("i", "[1, 2, 3]",
        Seq(createPattern(nodes = Seq(createNode("n"))),
          removeLabel("x", "L", "M"),
          delete("x", forced = true),
          setNodeProperty("n", "prop", "i")))
      .allNodeScan("x")
      .build())

  testPlan("subqueryForeach",
      new TestPlanBuilder()
        .produceResults("x")
        .subqueryForeach()
        .|.emptyResult()
        .|.create(createNode("n"))
        .|.argument("x")
        .allNodeScan("x")
        .build())

  testPlan("merge",
    new TestPlanBuilder()
      .produceResults("x")
      .merge(
        Seq(createNode("x"), createNode("y")),
        Seq(createRelationship("r", "x", "R", "y")),
        Seq(setNodeProperty("x", "prop", "42"), setNodePropertiesFromMap("x", "{prop: 42}")),
        Seq(setLabel("x", "L", "M"), setRelationshipProperty("r", "prop", "42"),
          setRelationshipPropertiesFromMap("r", "{prop: 42}")))
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build())

  testPlan("merge with lock",
    new TestPlanBuilder()
      .produceResults("x")
      .merge(
        relationships = Seq(createRelationship("r", "x", "R", "y")),
         lockNodes = Set("x", "y"))
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build())

  testPlan("anti",
    new TestPlanBuilder()
      .produceResults("x")
      .anti()
      .allNodeScan("x")
      .build())


  testPlan("optional",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.optional("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("cacheProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .cacheProperties("n.prop")
      .argument()
      .build())

  testPlan("cartesianProduct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build())

  testPlan("create",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(createNode("a", "A"), createNode("b"))
      .argument()
      .build())

  testPlan("create nodes and relationships",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(Seq(createNode("a", "A"), createNodeWithProperties("b", Seq("B"), "{node: true}")), Seq(createRelationship("r", "a", "R", "b", INCOMING, Some("{rel: true}"))))
      .argument()
      .build())

  testPlan("create with properties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(createNodeWithProperties("a", Seq("A"),"{foo: 42}"), createNodeWithProperties("b", Seq.empty, "{bar: 'hello'}"))
      .argument()
      .build())

  testPlan("procedureCall",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .procedureCall("test.proc2(5) YIELD foo")
      .procedureCall("test.proc1()")
      .argument()
      .build())

  testPlan("projectEndpoints",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .projectEndpoints("(a)-[r]-(b)", startInScope = true, endInScope = true)
      .projectEndpoints("(a)-[r]->(b)", startInScope = false, endInScope = true)
      .projectEndpoints("(a)-[r]-(b)", startInScope = true, endInScope = false)
      .projectEndpoints("(a)-[r*1..5]-(b)", startInScope = true, endInScope = false)
      .projectEndpoints("(a)-[r:A|B*1..5]-(b)", startInScope = true, endInScope = false)
      .argument()
      .build())

  testPlan("valueHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .valueHashJoin("x.bar = y.foo")
      .|.argument()
      .argument()
      .build())

  testPlan("nodeHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nodeHashJoin("x", "y")
      .|.nodeHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build())

  testPlan("rightOuterHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .rightOuterHashJoin("x", "y")
      .|.rightOuterHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build())

  testPlan("leftOuterHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .leftOuterHashJoin("x", "y")
      .|.leftOuterHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build())

  testPlan("emptyResult",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .emptyResult()
      .argument()
      .build())

  testPlan("eager",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .eager()
      .eager(Seq(EagernessReason.UpdateStrategyEager))
      .eager(Seq(EagernessReason.Unknown))
      .eager(Seq(
        EagernessReason.DeleteOverlap(Seq("n", "m")),
        EagernessReason.OverlappingSetLabels(Seq("X")),
        EagernessReason.OverlappingDeletedLabels(Seq("Foo", "Bar")),
      ))
      .argument()
      .build())

  testPlan("errorPlan",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .errorPlan(TestException())
      .argument()
      .build())

  testPlan("sort",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .sort(Seq(Ascending("x"), Ascending("y")))
      .sort(Seq(Descending("x")))
      .argument()
      .build())

  testPlan("partialSort",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x"), Ascending("y")), Seq(Ascending("xxx"), Descending("y")))
      .partialSort(Seq(Descending("x")), Seq(Ascending("x"), Ascending("y")))
      .partialSort(Seq(Descending("x")), Seq(Ascending("x")), 10)
      .argument()
      .build())

  testPlan("top",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .top(Seq(Ascending("xxx"), Descending("y")), 100)
      .top(Seq(Ascending("x"), Ascending("y")), 42)
      .argument()
      .build())

  testPlan("top1WithTies",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .top1WithTies(Seq(Ascending("xxx"), Descending("y")))
      .top1WithTies(Seq(Ascending("x"), Ascending("y")))
      .argument()
      .build())

  testPlan("partialTop",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x"), Ascending("y")), Seq(Ascending("xxx"), Descending("y")), 100)
      .partialTop(Seq(Descending("x")), Seq(Ascending("x"), Ascending("y")), 42)
      .partialTop(Seq(Descending("x")), Seq(Ascending("x"), Ascending("y")), 42, 17)
      .argument()
      .build())

  testPlan("distinct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .distinct("x AS y", "1 + n.foo AS z")
      .argument()
      .build())

  testPlan("orderedDistinct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedDistinct(Seq("x"), "x AS y", "1 + n.foo AS z")
      .orderedDistinct(Seq("1 + n.foo"), "x AS y", "1 + n.foo AS z")
      .argument()
      .build())

  testPlan("projection",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .projection("x AS y", "1 + n.foo AS z")
      .projection("cacheR[r.prop] AS rel")
      .projection("cacheR[r.prop] AS `r.prop`")
      .projection("cacheN[n.prop] AS node")
      .projection("cache[n.prop] AS node")
      .projection("cacheFromStore[n.prop] AS node")
      .projection("cacheNFromStore[n.prop] AS node")
      .projection("cacheRFromStore[r.prop] AS node")
      .argument()
      .build())

  testPlan("unwind",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .unwind("[x, 42, y.prop] AS y")
      .argument()
      .build())

  testPlan("union",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .union()
      .|.argument()
      .argument()
      .build())

  testPlan("orderedUnion",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedUnion(Seq(Ascending("x")))
      .|.argument()
      .argument()
      .build())

  testPlan("relationshipCountFromCountStore",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipCountFromCountStore("x", None, Seq("RelType"), None, "a", "b")
      .relationshipCountFromCountStore("x", Some("Start"), Seq("RelType", "FooBar"), Some("End"))
      .build())

  testPlan("nodeCountFromCountStore",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeCountFromCountStore("x", Seq(Some("Label")), "a", "b")
      .apply()
      .|.nodeCountFromCountStore("x", Seq(Some("Label"), None, Some("Babel")), "a")
      .nodeCountFromCountStore("x", Seq())
      .build())

  testPlan("detachDeleteNode",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .detachDeleteNode("x")
      .argument()
      .build())

  testPlan("deleteRelationship",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .deleteRelationship("x")
      .argument()
      .build())

  testPlan("setProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setProperty("x", "prop", "42")
      .setProperty("head([x])", "prop", "42")
      .argument()
      .build())

  testPlan(
    "setPropertyExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setPropertyExpression(varFor("x"), "prop", "42")
      .argument()
      .build()
  )

  testPlan("setNodeProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodeProperty("x", "prop", "42")
      .argument()
      .build())

  testPlan("setRelationshipProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipProperty("x", "prop", "42")
      .argument()
      .build())

  testPlan("setProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build())

  testPlan(
    "setPropertiesExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setPropertiesExpression(varFor("x"), ("p1", "42"), ("p1", "42"))
      .argument()
      .build()
  )

  testPlan("setNodeProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodeProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build())

  testPlan("setRelationshipProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build())

  testPlan("setPropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setPropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = true)
      .argument()
      .build())

  testPlan("setNodePropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodePropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = true)
      .argument()
      .build())

  testPlan("setRelationshipPropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipPropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = false)
      .argument()
      .build())

  testPlan("filter",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .filter("x.foo > 42", "true <> false")
      .argument()
      .build())

  testPlan("nonFuseable",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonFuseable()
      .argument()
      .build())

  testPlan("injectCompilationError",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .injectCompilationError()
      .argument()
      .build())

  testPlan("nonPipelined",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonPipelined()
      .argument()
      .build())

  testPlan("nonPipelinedHead",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonPipelinedHead()
      .argument()
      .build())

  testPlan("prober",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .prober(Prober.NoopProbe)
      .argument()
      .build())

  testPlan("nodeByIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeByIdSeek("y", Set("x"), 25)
      .nodeByIdSeek("x", Set(), 23, 22.0, -1)
      .build())

  testPlan("undirectedRelationshipByIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.undirectedRelationshipByIdSeek("r2", "x", "y", Set("x"), 25)
      .undirectedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
      .build())

  testPlan("pointDistanceNodeIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointDistanceNodeIndexSeek("y", "L", "prop", "{x: 1.0, y: 2.0, crs: 'cartesian'}", 10, argumentIds = Set("x"), getValue = GetValue)
      .pointDistanceNodeIndexSeek("x", "L", "prop","{x: 0.0, y: 1.0, crs: 'cartesian'}", 100, indexOrder = IndexOrderDescending)
      .build())

  testPlan("pointBoundingBoxNodeIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointBoundingBoxNodeIndexSeek("y", "L", "prop", "{x: 1.0, y: 2.0, crs: 'cartesian'}", "{x: 10.0, y: 20.0, crs: 'cartesian'}", argumentIds = Set("x"), getValue = GetValue)
      .pointBoundingBoxNodeIndexSeek("x", "L", "prop","{x: 0.0, y: 1.0, crs: 'cartesian'}", "{x: 100.0, y: 100.0, crs: 'cartesian'}", indexOrder = IndexOrderDescending)
      .build())

  testPlan("pointBoundingBoxRelationshipIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointBoundingBoxRelationshipIndexSeek("y", "y1", "y2", "L", "prop", "{x: 1.0, y: 2.0, crs: 'cartesian'}", "{x: 10.0, y: 20.0, crs: 'cartesian'}", argumentIds = Set("x"), getValue = GetValue)
      .pointBoundingBoxRelationshipIndexSeek("x", "x1", "y1",  "L", "prop","{x: 0.0, y: 1.0, crs: 'cartesian'}", "{x: 100.0, y: 100.0, crs: 'cartesian'}", directed = false, indexOrder = IndexOrderDescending)
      .build())

  testPlan("directedRelationshipByIdSeek",
           new TestPlanBuilder()
             .produceResults("x", "y")
             .apply()
             .|.directedRelationshipByIdSeek("r2", "x", "y", Set("x"), 25)
             .directedRelationshipByIdSeek("r1", "x", "y", Set(), 23, 22.0, -1)
             .build())

  testPlan("relationshipTypeScan",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipTypeScan("(x)-[r:R]-(y)")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build())

  testPlan("relationshipTypeScan with providedOrder",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipTypeScan("(x)-[r:R]-(y)", IndexOrderDescending)
      .relationshipTypeScan("(x)-[r:R]->(y)", IndexOrderAscending)
      .build())

  // Formatting paramExpr and customQueryExpression is currently not supported.
  // These cases will need manual fixup.
  testPlan("nodeIndexOperator", {
    val builder = new TestPlanBuilder().produceResults("x", "y")

    // NodeIndexSeek
    builder
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 20 OR 30)")
      .apply()
      .|.nodeIndexOperator("x:Honey(prop > 20)")
      .apply()
      .|.nodeIndexOperator("x:Honey(10 < prop < 20)")
      .apply()
      .|.nodeIndexOperator("x:Honey(10 < prop <= 20)", indexType = IndexType.RANGE)
      .apply()
      .|.nodeIndexOperator("x:Honey(10 <= prop < 20)")
      .apply()
      .|.nodeIndexOperator("x:Honey(10 <= prop <= 20)")
      .apply()
      .|.nodeIndexOperator("x:Honey(prop >= 20)", indexOrder = IndexOrderNone)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop < 20)", getValue = _ => DoNotGetValue)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop <= 20)", getValue = {case "prop" => GetValue})
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 10, prop2 = '20')", indexOrder = IndexOrderDescending, getValue = Map("prop" -> GetValue, "prop2" -> DoNotGetValue))
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')", argumentIds = Set("a", "b"))
      .apply()
      .|.nodeIndexOperator("x:Label(text STARTS WITH 'as')", indexOrder = IndexOrderAscending, indexType = IndexType.TEXT)
      .nodeIndexOperator("x:Honey(prop2 = 10, prop)")
      .build()
  })

  // Split into 2 tests to avoid StackOverflow exceptions on too long method call chains.
  testPlan("nodeIndexOperator 2", {
    val builder = new TestPlanBuilder().produceResults("r")

    // NodeUniqueIndexSeek
    builder
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 20)", unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 20 OR 30)", unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop > 20)", unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop >= 20)", indexOrder = IndexOrderNone, unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop < 20)", getValue = _ => DoNotGetValue, unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop <= 20)", getValue = _ => GetValue, unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 10, prop2 = '20')", indexOrder = IndexOrderDescending, unique = true)
      .apply()
      .|.nodeIndexOperator("x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')", argumentIds = Set("a", "b"), unique = true)
      .nodeIndexOperator("x:Label(text STARTS WITH 'as')", indexOrder = IndexOrderAscending, unique = true)
      .build()
  })

  testPlan("nodeIndexOperator - scans", {
    val builder = new TestPlanBuilder().produceResults("r")

    // NodeIndexScan
    builder
      .apply()
      .|.nodeIndexOperator("x:Honey(calories)")
      .apply()
      .|.nodeIndexOperator("x:Honey(calories, taste)", getValue = _ => GetValue)
      .apply()
      .|.nodeIndexOperator("x:Honey(calories, taste)", indexOrder = IndexOrderDescending)
      .apply()
      .|.nodeIndexOperator("x:Honey(calories, taste)", argumentIds = Set("a", "b"))

    // NodeIndexContainsScan
    builder
      .apply()
      .|.nodeIndexOperator("x:Label(text CONTAINS 'as')", indexType = IndexType.TEXT)
      .apply()
      .|.nodeIndexOperator("x:Honey(text CONTAINS 'as')", getValue = _ => GetValue)
      .apply()
      .|.nodeIndexOperator("x:Honey(text CONTAINS 'as')", indexOrder = IndexOrderDescending)
      .apply()
      .|.nodeIndexOperator("x:Honey(text CONTAINS 'as')", argumentIds = Set("a", "b"))

    // NodeIndexEndsWithScan
    builder
      .apply()
      .|.nodeIndexOperator("x:Label(text ENDS WITH 'as')")
      .apply()
      .|.nodeIndexOperator("x:Honey(text ENDS WITH 'as')", getValue = _ => GetValue)
      .apply()
      .|.nodeIndexOperator("x:Honey(text ENDS WITH 'as')", indexOrder = IndexOrderDescending)
      .nodeIndexOperator("x:Honey(text ENDS WITH 'as')", argumentIds = Set("a", "b"), indexType = IndexType.TEXT)

    builder.build()
  })

  // Formatting paramExpr and customQueryExpression is currently not supported.
  // These cases will need manual fixup.
  testPlan("relationshipIndexOperator", {
    val builder = new TestPlanBuilder().produceResults("r")

    // directed RelationshipIndexSeek
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop >= 20)]->(y)", indexOrder = IndexOrderNone)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop < 20)->(y)", getValue = _ => DoNotGetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop <= 20)->(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 10, prop2 = '20')->(y)", indexOrder = IndexOrderDescending)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')->(y)", argumentIds = Set("a", "b"))
      .relationshipIndexOperator("(x)-[r:Label(text STARTS WITH 'as')->(y)", indexOrder = IndexOrderAscending)
      .build()
  })

  // Split into 2 tests to avoid StackOverflow exceptions on too long method call chains.
  testPlan("relationshipIndexOperator 2", {
    val builder = new TestPlanBuilder().produceResults("r")

    // undirected RelationshipIndexSeek
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop >= 20)]-(y)", indexOrder = IndexOrderNone)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop < 20)-(y)", getValue = _ => DoNotGetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop <= 20)-(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 10, prop2 = '20')-(y)", indexOrder = IndexOrderDescending, getValue = {case "prop" => GetValue; case "prop2" => DoNotGetValue})
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')-(y)", argumentIds = Set("a", "b"))
      .relationshipIndexOperator("(x)-[r:Label(text STARTS WITH 'as')-(y)", indexOrder = IndexOrderAscending)
      .build()
  })

  testPlan("relationshipIndexOperator - scan", {
    val builder = new TestPlanBuilder().produceResults("r")

    // directed indexScan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories)->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories, taste)->(y)", getValue = {case "calories" => GetValue; case _ => DoNotGetValue})
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories, taste)->(y)", indexOrder = IndexOrderDescending)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories, taste)->(y)", argumentIds = Set("a", "b"))

    // undirected indexScan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories)-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories, taste)-(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(calories, taste)-(y)", indexOrder = IndexOrderDescending)
      .relationshipIndexOperator("(x)-[r:Honey(calories, taste)-(y)", argumentIds = Set("a", "b"))
      .build()
  })

  testPlan("relationshipIndexOperator - contains", {
    val builder = new TestPlanBuilder().produceResults("r")

    // directed contains scan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Label(text CONTAINS 'as')->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')->(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')->(y)", indexOrder = IndexOrderDescending)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')->(y)", argumentIds = Set("a", "b"))

    // undirected contains scan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Label(text CONTAINS 'as')-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')-(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')-(y)", indexOrder = IndexOrderDescending)
      .relationshipIndexOperator("(x)-[r:Honey(text CONTAINS 'as')-(y)", argumentIds = Set("a", "b"))
      .build()
  })

  testPlan("relationshipIndexOperator - ends with", {
    val builder = new TestPlanBuilder().produceResults("r")

    // directed ends with scan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Label(text ENDS WITH 'as')->(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')->(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')->(y)", indexOrder = IndexOrderDescending)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')->(y)", argumentIds = Set("a", "b"))

    // undirected ends with scan
    builder
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Label(text ENDS WITH 'as')-(y)")
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')-(y)", getValue = _ => GetValue)
      .apply()
      .|.relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')-(y)", indexOrder = IndexOrderDescending)
      .relationshipIndexOperator("(x)-[r:Honey(text ENDS WITH 'as')-(y)", argumentIds = Set("a", "b"))

    builder.build()
  })

  testPlan("multiNodeIndexSeekOperator",
    new TestPlanBuilder()
     .produceResults("n", "m")
     .multiNodeIndexSeekOperator(_.nodeIndexSeek("n:Label(prop=5)"),
                                 _.nodeIndexSeek("m:Label(prop=6)"))
     .build())

  testPlan("triadicSelection",
    new TestPlanBuilder()
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-[r2]->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-[r1]->(y)")
      .allNodeScan("x")
      .build())

  testPlan("triadicBuild",
    new TestPlanBuilder()
      .produceResults("n")
      .triadicFilter(42, positivePredicate = true, "n", "n")
      .triadicBuild(42, "n", "n")
      .allNodeScan("n")
      .build())

  testPlan("triadicFilter",
    new TestPlanBuilder()
      .produceResults("n")
      .triadicFilter(42, positivePredicate = true, "n", "n")
      .triadicBuild(42, "n", "n")
      .allNodeScan("n")
      .build())


  testPlan("injectValue",
    new TestPlanBuilder()
      .produceResults("x")
      .injectValue("x", "null")
      .argument()
      .build())

  testPlan("assertSameNode",
    new TestPlanBuilder()
      .produceResults("x")
      .assertSameNode("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build())

  testPlan("deleteNode",
    new TestPlanBuilder()
      .produceResults("n")
      .deleteNode("n")
      .allNodeScan("n")
      .build())

  testPlan("deletePath",
    new TestPlanBuilder()
      .produceResults("p")
      .deletePath("p")
      .argument("p")
      .build())

  testPlan("detachDeletePath",
    new TestPlanBuilder()
      .produceResults("p")
      .deletePath("p")
      .argument("p")
      .build())

  testPlan("deleteExpression",
    new TestPlanBuilder()
      .produceResults("e")
      .deletePath("e")
      .argument("e")
      .build())

  testPlan("detachDeleteExpression",
    new TestPlanBuilder()
      .produceResults("e")
      .deletePath("e")
      .argument("e")
      .build())

  testPlan("removeLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .removeLabels("n", "Label")
      .argument("n")
      .build())


  testPlan("setLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .setLabels("n", "Label", "OtherLabel")
      .argument("n")
      .build())

  testPlan("loadCSV",
    new TestPlanBuilder()
      .produceResults("x", "var")
      .loadCSV("url", "var", NoHeaders, Some("-"))
      .loadCSV("url", "var", HasHeaders, None)
      .allNodeScan("x")
      .build())

  testPlan("transactionForeach",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build())

  testPlan("transactionForeach with batchSize",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach(10)
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build())

  testPlan("transactionApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build())

  testPlan("transactionApply with batchSize",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(42)
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build())

  private def interpretPlanBuilder(code: String): LogicalPlan = {
    val completeCode =
      s"""
         |new org.neo4j.cypher.internal.logical.builder.TestPlanBuilder()
         |$code""".stripMargin
    val res = Array[AnyRef](null)

    val settings = new Settings()
    settings.usejavacp.value = true
    val interpreter = new IMain(settings)

    try {
      interpreter.beSilentDuring {
        // imports
        interpreter.interpret(
          """import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.delete
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperty
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setPropertyFromMap
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
            |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
            |import org.neo4j.cypher.internal.expressions.SemanticDirection.{INCOMING, OUTGOING, BOTH}
            |import org.neo4j.cypher.internal.logical.plans._
            |import org.neo4j.cypher.internal.logical.builder.TestException
            |import org.neo4j.cypher.internal.ir.HasHeaders
            |import org.neo4j.cypher.internal.ir.NoHeaders
            |import org.neo4j.cypher.internal.ir.EagernessReason
            |import org.neo4j.graphdb.schema.IndexType
            |""".stripMargin)
        interpreter.bind("result", "Array[AnyRef]", res)
      }
      interpreter.interpret(s"result(0) = $completeCode")
    } catch {
      case t: Throwable =>
        fail("Failed to interpret generated code: ", t)
    } finally {
      interpreter.close()
    }
    res(0).asInstanceOf[LogicalPlan]
  }

  /**
   * Test that for each method of AbstractLogicalPlanBuilder that generated a plan
   * we have a test calling [[testPlan()]] here.
   * This is done via reflection.
   */
  test("all the tests exist") {
    val methodsWeCantTest = Set(
      "filterExpression",
      "appendAtCurrentIndent",
      "nestedPlanExistsExpressionProjection",
      "nestedPlanCollectExpressionProjection",
      "pointDistanceNodeIndexSeekExpr",
      "pointBoundingBoxNodeIndexSeekExpr",
      "pointBoundingBoxRelationshipIndexSeekExpr",
      "resetIndent"
    )
    withClue("tests missing for these operators:") {
      val methods = classOf[AbstractLogicalPlanBuilder[_, _]].getDeclaredMethods.filter { m =>
        val modifiers = m.getModifiers
        m.getGenericReturnType.getTypeName == "IMPL" && Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers) && !Modifier.isAbstract(modifiers)
      }
      methods should not be empty
      methods.map { m =>
        val name = m.getName
        val index = name.indexOf("$") // filter out the $bar method
        val end = if (index == -1) name.length else index
        name.substring(0, end)
      }.filter(_.nonEmpty).toSet[String] -- methodsWeCantTest -- testedOperators should be(empty)
    }
  }

  /**
   * Tests a plan by getting the string representation and then using scala REPL to execute that code, which yields a `rebuiltPlan`.
   * Compare that plan against the original plan.
   */
  private def testPlan(name: String, buildPlan: => LogicalPlan): Unit = {
    testedOperators.add(name)
    test(name) {
      val plan = buildPlan // to avoid running out of stack while compiling the huge LogicalPlanToLogicalPlanBuilderStringTest constructor
      val code = LogicalPlanToPlanBuilderString(plan)
      val rebuiltPlan = interpretPlanBuilder(code)
      if (rebuiltPlan == null) {
        throw new RuntimeException(s"This code did not produce a plan:\n$code")
      }
      rebuiltPlan should equal(plan)
    }
  }
}

case class TestException() extends RuntimeException()
