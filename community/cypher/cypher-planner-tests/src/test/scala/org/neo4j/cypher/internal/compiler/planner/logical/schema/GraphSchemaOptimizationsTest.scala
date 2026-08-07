/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.schema

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext

class GraphSchemaOptimizationsTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  test("should not prune labels if there are no implied labels") {
    val gso = givenImpliedLabels()
    gso.pruneImpliedLabels(labelNames("A", "B")) shouldEqual labelNames("A", "B")
    gso.pruneImpliedLabels(labelInfo("x" -> Set("A", "B"))) shouldEqual labelInfo("x" -> Set("A", "B"))
  }

  test("should prune labels if there is an implied label") {
    val gso = givenImpliedLabels("A" -> Set("B"))
    gso.pruneImpliedLabels(labelNames("A", "B")) shouldEqual labelNames("A")
    gso.pruneImpliedLabels(labelInfo("x" -> Set("A", "B"))) shouldEqual labelInfo("x" -> Set("A"))
  }

  test("should prune multiple unrelated implied labels") {
    val gso = givenImpliedLabels(
      "A" -> Set("B"),
      "C" -> Set("D")
    )
    gso.pruneImpliedLabels(labelNames("A", "B", "C", "D")) shouldEqual labelNames("A", "C")

    val li = labelInfo(
      "x" -> Set("A", "B"),
      "y" -> Set("C", "D")
    )
    gso.pruneImpliedLabels(li) shouldEqual labelInfo("x" -> Set("A"), "y" -> Set("C"))
  }

  test("should prune multiple related implied labels") {
    val gso = givenImpliedLabels(
      "A" -> Set("B", "C", "D"),
      "E" -> Set("F", "G", "H")
    )
    gso.pruneImpliedLabels(labelNames("A", "B", "D", "E", "H")) shouldEqual labelNames("A", "E")

    val li = labelInfo(
      "x" -> Set("A", "B", "E", "F"),
      "y" -> Set("A", "C")
    )
    gso.pruneImpliedLabels(li) shouldEqual labelInfo("x" -> Set("A", "E"), "y" -> Set("A"))
  }

  test("should keep labels if they are not implied") {
    val gso = givenImpliedLabels("A" -> Set("B"))
    gso.pruneImpliedLabels(labelNames("A", "B", "Other")) shouldEqual labelNames("A", "Other")
    gso.pruneImpliedLabels(labelInfo("x" -> Set("A", "B", "Other"))) shouldEqual labelInfo("x" -> Set("A", "Other"))
  }

  test("should prune constrained labels over 2 layers") {
    val gso = givenImpliedLabels("A" -> Set("B"), "C" -> Set("B", "D"))
    gso.pruneConstrainedLabels(labelNameSeq("A", "B", "C", "Other")) shouldEqual labelNameSeq("B", "Other")
  }

  test("should not add implied labels if there are none defined") {
    val gso = givenImpliedLabels()
    gso.addImpliedLabels(labelNames("A", "B", "Other")) shouldEqual labelNames("A", "B", "Other")
    gso.addImpliedLabels(labelInfo("x" -> Set("A", "B", "Other"))) shouldEqual labelInfo("x" -> Set("A", "B", "Other"))
  }

  test("should add implied labels") {
    val gso = givenImpliedLabels("A" -> Set("B"), "C" -> Set("D", "E"))
    gso.addImpliedLabels(labelNames("A", "C", "Other")) shouldEqual
      labelNames("A", "B", "C", "D", "E", "Other")
    gso.addImpliedLabels(labelInfo("x" -> Set("A", "Other"), "y" -> Set("C"))) shouldEqual
      labelInfo("x" -> Set("A", "B", "Other"), "y" -> Set("C", "D", "E"))
  }

  private def givenImpliedLabels(impliedLabels: (String, Set[String])*): GraphSchemaOptimizations.Enabled = {
    new GraphSchemaOptimizations.Enabled(mockPlanContext(impliedLabels.toMap))
  }

  private def mockPlanContext(impliedLabels: Map[String, Set[String]]): NotImplementedPlanContext = {
    new NotImplementedPlanContext {
      override def getNodeLabelConstraints(constrainedLabel: String): Set[String] =
        impliedLabels.getOrElse(constrainedLabel, Set.empty)
    }
  }

  private def labelNames(labels: String*): Set[LabelName] = labels.map(labelName(_)).toSet

  private def labelNameSeq(labels: String*): Seq[LabelName] = labels.map(labelName(_))

  private def labelInfo(labelsForVariable: (String, Set[String])*): LabelInfo = {
    labelsForVariable.map {
      case (variable, labels) => varFor(variable) -> labelNames(labels.toSeq: _*)
    }.toMap
  }
}
