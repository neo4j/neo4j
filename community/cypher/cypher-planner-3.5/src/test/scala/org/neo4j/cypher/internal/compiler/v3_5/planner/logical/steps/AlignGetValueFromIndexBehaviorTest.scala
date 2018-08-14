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

  val indexSeekWithValues = NodeIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
  val indexSeekWithoutValues = NodeIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
  val uniqueIndexSeekWithValues = NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
  val uniqueIndexSeekWithoutValues = NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
  val indexContainsScanWithValues = NodeIndexContainsScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexContainsScanWithoutValues = NodeIndexContainsScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexEndsWithScanWithValues = NodeIndexEndsWithScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexEndsWithScanWithoutValues = NodeIndexEndsWithScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
    StringLiteral("foo")(pos),
    Set.empty)
  val indexScanWithValues = NodeIndexScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), GetValue),
    Set.empty)
  val indexScanWithoutValues = NodeIndexScan(
    "n",
    LabelToken("Awesome", LabelId(0)),
    IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), DoNotGetValue),
    Set.empty)

  val indexOperatorTuples = Seq(
    (indexSeekWithValues, indexSeekWithoutValues),
    (uniqueIndexSeekWithValues, uniqueIndexSeekWithoutValues),
    (indexContainsScanWithValues, indexContainsScanWithoutValues),
    (indexEndsWithScanWithValues, indexEndsWithScanWithoutValues),
    (indexScanWithValues, indexScanWithoutValues)
  )

  for((withValues, withoutValues) <- indexOperatorTuples) {

    test(s"should keep GetValue on ${withValues.getClass.getSimpleName} with usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(withValues) should equal(withValues)
      }
    }

    test(s"should keep GetValue on ${withValues.getClass.getSimpleName} with usage of that property nested in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("stuff" -> listOf(prop("n", "prop")))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(withValues) should equal(withValues)
      }
    }

    test(s"should keep DoNotGetValue on ${withValues.getClass.getSimpleName} with usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "prop"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(withoutValues) should equal(withoutValues)
      }
    }

    test(s"should set DoNotGetValue on ${withValues.getClass.getSimpleName} without usage of that property in horizon") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "anotherProp"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(withValues) should equal(withoutValues)
      }
    }

    test(s"should set DoNotGetValue on ${withValues.getClass.getSimpleName} without usage of that property in horizon, if nested") {
      new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
        // Given
        val query = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n" -> prop("n", "anotherProp"))))
        val updater = alignGetValueFromIndexBehavior(query, context.logicalPlanProducer, Attributes(idGen))

        updater(Distinct(withValues, Map.empty)) should equal(Distinct(withoutValues, Map.empty))
      }
    }

  }

}
