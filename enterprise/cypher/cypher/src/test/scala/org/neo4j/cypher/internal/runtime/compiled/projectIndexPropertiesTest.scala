/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.attribution.Attributes
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class projectIndexPropertiesTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

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

  val expectedProjections = Map("n.prop" -> prop("n", "prop"))

  val indexOperatorTuples = Seq(
    (indexSeekWithValues, indexSeekWithoutValues),
    (uniqueIndexSeekWithValues, uniqueIndexSeekWithoutValues),
    (indexContainsScanWithValues, indexContainsScanWithoutValues),
    (indexEndsWithScanWithValues, indexEndsWithScanWithoutValues),
    (indexScanWithValues, indexScanWithoutValues)
  )

  for((withValues, withoutValues) <- indexOperatorTuples) {

    test(s"should introduce projection for ${withValues.getClass.getSimpleName} with index properties") {
      val attr = Attributes(idGen)
      val updater = projectIndexProperties(attr)
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(withValues, emptyTable)
      newPlan should equal(Projection(withoutValues, expectedProjections)(idGen))
      // We have to use the exact var in the plan so that the input position is the same
      val varInNewPlan = newPlan.asInstanceOf[Projection].expressions("n.prop").asInstanceOf[Property].map.asInstanceOf[Variable]
      newTable.isNode(varInNewPlan) should be(true)
    }

    test(s"should not introduce projection for ${withoutValues.getClass.getSimpleName} without index properties") {
      val attr = Attributes(idGen)
      val updater = projectIndexProperties(attr)
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(withoutValues, emptyTable)
      newPlan should equal(withoutValues)
    }

  }

}
