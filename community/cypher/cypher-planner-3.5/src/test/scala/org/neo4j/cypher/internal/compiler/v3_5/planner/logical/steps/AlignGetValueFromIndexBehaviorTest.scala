/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.ir.v3_5.{RegularPlannerQuery, RegularQueryProjection}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.attribution.Attributes
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class AlignGetValueFromIndexBehaviorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  type IndexOperator = GetValueFromIndexBehavior => IndexLeafPlan

  val indexSeek: IndexOperator = getValue => NodeIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty, IndexOrderNone)
  val uniqueIndexSeek: IndexOperator = getValue => NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty, IndexOrderNone)
  val indexContainsScan: IndexOperator = getValue => NodeIndexContainsScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexEndsWithScan: IndexOperator = getValue => NodeIndexEndsWithScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexScan: IndexOperator = getValue => NodeIndexScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue),
    Set.empty, IndexOrderNone)

  val indexOperators = Seq(indexSeek, uniqueIndexSeek, indexContainsScan, indexEndsWithScan, indexScan)

  for(indexOperator <- indexOperators) {

    val doNotGetValues = indexOperator(DoNotGetValue)
    val canGetValues = indexOperator(CanGetValue)
    val getValues = indexOperator(GetValue)
    val operatorName = getValues.getClass.getSimpleName

    test(s"should set GetValue on $operatorName with usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(canGetValues) should equal(getValues)
      }
    }

    test(s"should set GetValue on $operatorName with usage of that property nested in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("stuff" -> listOf(prop("n", "prop")))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(canGetValues) should equal(getValues)
      }
    }

    test(s"should keep DoNotGetValue on $operatorName with usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(doNotGetValues) should equal(doNotGetValues)
      }
    }

    test(s"should set DoNotGetValue on $operatorName without usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "anotherProp"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(canGetValues) should equal(doNotGetValues)
      }
    }

    test(s"should set DoNotGetValue on $operatorName without usage of that property in horizon, if nested") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "anotherProp"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(Distinct(canGetValues, Map.empty)) should equal(Distinct(doNotGetValues, Map.empty))
      }
    }

    test(s"should set DoNotGetValue on $operatorName if plan inside a union") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(Union(canGetValues, canGetValues)) should equal(Union(doNotGetValues, doNotGetValues))
      }
    }

    test(s"should set DoNotGetValue on $operatorName if plan inside a selection inside union") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        val ands = Ands(Set(ListLiteral(Seq.empty)(pos)))(pos)
        updater(Union(Selection(ands, canGetValues), Selection(ands, canGetValues))) should equal(
          Union(Selection(ands, doNotGetValues), Selection(ands, doNotGetValues)))
      }
    }
  }

}
