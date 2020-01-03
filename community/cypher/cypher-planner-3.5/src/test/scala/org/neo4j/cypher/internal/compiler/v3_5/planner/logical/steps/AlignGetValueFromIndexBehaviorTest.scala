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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.v3_5.Predicate
import org.neo4j.cypher.internal.ir.v3_5.QueryGraph
import org.neo4j.cypher.internal.ir.v3_5.Selections
import org.neo4j.cypher.internal.ir.v3_5.RegularPlannerQuery
import org.neo4j.cypher.internal.ir.v3_5.RegularQueryProjection
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.attribution.Attributes
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.LabelId
import org.neo4j.cypher.internal.v3_5.util.PropertyKeyId

class AlignGetValueFromIndexBehaviorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  type IndexOperator = GetValueFromIndexBehavior => IndexLeafPlan

  val indexSeek: IndexOperator = getValue => IndexSeek("n:Awesome(prop = 42)", getValue)
  val uniqueIndexSeek: IndexOperator = getValue => NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty,
    IndexOrderNone)
  val indexContainsScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop CONTAINS 'foo')", getValue)
  val indexEndsWithScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop ENDS WITH 'foo')", getValue)
  val indexScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop)", getValue)

  val indexOperators = Seq(indexSeek, uniqueIndexSeek, indexContainsScan, indexEndsWithScan, indexScan)

  for (indexOperator <- indexOperators) {

    val doNotGetValues = indexOperator(DoNotGetValue)
    val canGetValues = indexOperator(CanGetValue)
    val getValues = indexOperator(GetValue)
    val operatorName = getValues.getClass.getSimpleName

    test(s"should set GetValue on $operatorName with usage of that property in horizon") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property nested in horizon") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("stuff" -> listOf(prop("n", "prop")))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should keep DoNotGetValue on $operatorName with usage of that property in horizon") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(doNotGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))
        alignGetValueFromIndexBehavior(query, doNotGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(doNotGetValues)
      }
    }

    test(s"should set DoNotGetValue on $operatorName without usage of that property ") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "anotherProp"))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(doNotGetValues)
      }
    }

    test(s"should set DoNotGetValue on $operatorName without usage of that property, if nested") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val plan = Distinct(canGetValues, Map.empty)
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "anotherProp"))))
        alignGetValueFromIndexBehavior(query, plan, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(Distinct(doNotGetValues, Map.empty))
      }
    }

    test(s"should stop traversal (leave at CanGetValue) on $operatorName if plan inside a union") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val plan = Union(canGetValues, canGetValues)
        context.planningAttributes.solveds.set(plan.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))
        alignGetValueFromIndexBehavior(query, plan, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(plan)
      }
    }

    test(s"should stop traversal (leave at CanGetValue) on $operatorName if plan inside a selection inside union") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val ands = Ands(Set(ListLiteral(Seq.empty)(pos)))(pos)
        val plan = Union(Selection(ands, canGetValues), Selection(ands, canGetValues))
        context.planningAttributes.solveds.set(plan.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))
        alignGetValueFromIndexBehavior(query, plan, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(plan)
      }
    }

    test(s"should stop traversal (leave at CanGetValue) on $operatorName if plan inside union left deep tree") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val ands = Ands(Set(ListLiteral(Seq.empty)(pos)))(pos)
        val plan = Union(Union(Selection(ands, canGetValues), canGetValues), Selection(ands, canGetValues))
        context.planningAttributes.solveds.set(plan.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))
        alignGetValueFromIndexBehavior(query, plan, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(plan)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in another predicate") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(
          queryGraph = QueryGraph(selections = Selections(Set(Predicate(Set("n"), prop("n", "prop"))))),
          horizon = RegularQueryProjection(Map("foo" -> prop("n", "foo"))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set DoNotGetValue on $operatorName with only usage of that property in the solved predicate") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val queryGraph = QueryGraph(selections = Selections(Set(Predicate(Set("n"), prop("n", "prop")))))
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery(queryGraph = queryGraph))

        val query = RegularPlannerQuery(
          queryGraph = queryGraph,
          horizon = RegularQueryProjection(Map("foo" -> prop("n", "foo"))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(doNotGetValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in the solved predicate and in another predicate") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        val predicate = Predicate(Set("n"), prop("n", "prop"))
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery(queryGraph = QueryGraph(selections = Selections(Set(predicate)))))

        val query = RegularPlannerQuery(
          queryGraph = QueryGraph(selections = Selections(Set(predicate, Predicate(Set("n"), Equals(prop("n", "prop"), literalInt(1))(pos))))),
          horizon = RegularQueryProjection(Map("foo" -> prop("n", "foo"))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in the next query part (PassthroughAllHorizon)") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(
          horizon = PassthroughAllHorizon(),
          tail = Some(RegularPlannerQuery(
            horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in the next query part (Projection: n AS n)") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(
          horizon = RegularQueryProjection(Map("n" -> varFor("n"))),
          tail = Some(RegularPlannerQuery(
            horizon = RegularQueryProjection(Map("foo" -> prop("n", "prop"))))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in the next query part (Projection: n AS m)") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(
          horizon = RegularQueryProjection(Map("m" -> varFor("n"))),
          tail = Some(RegularPlannerQuery(
            horizon = RegularQueryProjection(Map("foo" -> prop("m", "prop"))))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property in the second next query part (Projection: n AS m, Projection: m AS o)") {
      new given().withLogicalPlanningContext { (cfg, context) =>
        context.planningAttributes.solveds.set(canGetValues.id, RegularPlannerQuery())

        val query = RegularPlannerQuery(
          horizon = RegularQueryProjection(Map("m" -> varFor("n"))),
          tail = Some(RegularPlannerQuery(
            horizon = RegularQueryProjection(Map("o" -> varFor("m"))),
            tail = Some(RegularPlannerQuery(
              horizon = RegularQueryProjection(Map("foo" -> prop("o", "prop"))))))))
        alignGetValueFromIndexBehavior(query, canGetValues, context.logicalPlanProducer, context.planningAttributes.solveds, Attributes(idGen)) should equal(getValues)
      }
    }

  }
}
