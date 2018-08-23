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

  type IndexOperator = GetValueFromIndexBehavior => IndexLeafPlan

  val indexSeek: IndexOperator = getValue => NodeIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
  val uniqueIndexSeek: IndexOperator = getValue => NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty)
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
    Set.empty)

  val expectedProjections = Map("n.prop" -> prop("n", "prop"))

  val indexOperators = Seq(indexSeek, uniqueIndexSeek, indexContainsScan, indexEndsWithScan, indexScan)

  for(indexOperator <- indexOperators) {

    val doNotGetValues = indexOperator(DoNotGetValue)
    val getValues = indexOperator(GetValue)
    val operatorName = getValues.getClass.getSimpleName

    test(s"should introduce projection for $operatorName with index properties") {
      val updater = projectIndexProperties
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(getValues, emptyTable)
      newPlan should equal(Projection(doNotGetValues, expectedProjections)(idGen))
      // We have to use the exact var in the plan so that the input position is the same
      val varInNewPlan = newPlan.asInstanceOf[Projection].expressions("n.prop").asInstanceOf[Property].map.asInstanceOf[Variable]
      newTable.isNode(varInNewPlan) should be(true)
    }

    test(s"should not introduce projection for $operatorName without index properties") {
      val updater = projectIndexProperties
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(doNotGetValues, emptyTable)
      newPlan should equal(doNotGetValues)
    }

  }

}
