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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingBtreeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHintType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.JoinHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.IndexHintException
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.JoinHintException

import scala.collection.JavaConverters.seqAsJavaListConverter

object VerifyBestPlan {
  private val prettifier = Prettifier(ExpressionStringifier())

  def apply(plan: LogicalPlan, expected: PlannerQueryPart, context: LogicalPlanningContext): Unit = {
    val constructed: PlannerQueryPart = context.planningAttributes.solveds.get(plan.id)

    if (expected != constructed) {
      val unfulfillableIndexHints = findUnfulfillableIndexHints(expected, context)
      val unfulfillableJoinHints = findUnfulfillableJoinHints(expected)
      val expectedWithoutHints = expected.withoutHints(unfulfillableIndexHints.map(_.hint) ++ unfulfillableJoinHints)
      if (expectedWithoutHints != constructed) {
        val a: PlannerQueryPart = expected.withoutHints(expected.allHints)
        val b: PlannerQueryPart = constructed.withoutHints(constructed.allHints)
        if (a != b) {
          // unknown planner issue failed to find plan (without regard for differences in hints)
          val moreDetails =
            (a, b) match {
              case (aSingle: RegularSinglePlannerQuery, bSingle: RegularSinglePlannerQuery) =>
                aSingle.pointOutDifference(bSingle, "Expected", "Actual")
              case _ => ""
            }

          throw new InternalException(s"Expected: \n$expected \n\nActual: \n$constructed\n\nPlan: \n${LogicalPlanToPlanBuilderString(plan)} \n\n$moreDetails")
        } else {
          // unknown planner issue failed to find plan matching hints (i.e. "implicit hints")
          val expectedHints = expected.allHints
          val actualHints = constructed.allHints
          val missing = expectedHints.diff(actualHints)
          val solvedInAddition = actualHints.diff(expectedHints)
          val inventedHintsAndThenSolvedThem = solvedInAddition.exists(!expectedHints.contains(_))
          if (missing.nonEmpty || inventedHintsAndThenSolvedThem) {
            def out(h: Set[Hint]) = h.map(prettifier.asString).mkString("`", ", ", "`")

            val details = if (missing.isEmpty)
              s"""Expected:
                 |${out(expectedHints)}
                 |
                 |Instead, got:
                 |${out(actualHints)}""".stripMargin
            else
              s"Could not solve these hints: ${out(missing)}"

            val message =
              s"""Failed to fulfil the hints of the query.
                 |$details
                 |
                 |Plan $plan""".stripMargin

            throw new HintException(message)
          }
        }
      } else {
        processUnfulfilledIndexHints(context, unfulfillableIndexHints)
        processUnfulfilledJoinHints(plan, context, unfulfillableJoinHints)
      }
    }
  }

  private def processUnfulfilledIndexHints(context: LogicalPlanningContext, hints: Set[UnfulfillableIndexHint]): Unit = {
    if (hints.nonEmpty) {
      // hints referred to non-existent indexes ("explicit hints")
      if (context.useErrorsOverWarnings) {
        throw hints.head.toException
      } else {
        hints.foreach { hint =>
            context.notificationLogger.log(hint.toNotification)
        }
      }
    }
  }

  private def processUnfulfilledJoinHints(plan: LogicalPlan, context: LogicalPlanningContext, hints: Set[UsingJoinHint]): Unit = {
    if (hints.nonEmpty) {
      // we were unable to plan hash join on some requested nodes
      if (context.useErrorsOverWarnings) {
        throw new JoinHintException(s"Unable to plan hash join. Instead, constructed\n$plan")
      } else {
        hints.foreach { hint =>
          context.notificationLogger.log(JoinHintUnfulfillableNotification(hint.variables.map(_.name).toIndexedSeq))
        }
      }
    }
  }

  case class UnfulfillableIndexHint(hint: UsingIndexHint, entityType: EntityType) {
    def toNotification: IndexHintUnfulfillableNotification =
      IndexHintUnfulfillableNotification(hint.variable.name, hint.labelOrRelType.name, hint.properties.map(_.name), entityType, hint.indexType)

    def toException: IndexHintException = {
      val exceptionIndexType = hint.indexType match {
        case UsingAnyIndexType => IndexHintIndexType.ANY
        case UsingBtreeIndexType => IndexHintIndexType.BTREE
        case UsingTextIndexType => IndexHintIndexType.TEXT
      }
      new IndexHintException(hint.variable.name, hint.labelOrRelType.name, hint.properties.map(_.name).asJava, entityType, exceptionIndexType)
    }
  }

  private def findUnfulfillableIndexHints(query: PlannerQueryPart, context: LogicalPlanningContext): Set[UnfulfillableIndexHint] = {
    
    val planContext = context.planContext
    val semanticTable = context.semanticTable
    
    def nodeIndexHintFulfillable(labelOrRelType: LabelOrRelTypeName, properties: Seq[PropertyKeyName], indexHintType: UsingIndexHintType): Boolean = {
      val labelName = labelOrRelType.name
      val propertyNames = properties.map(_.name)

      def btreeExists = planContext.btreeIndexExistsForLabelAndProperties(labelName, propertyNames)
      def textExists = context.planningTextIndexesEnabled && planContext.textIndexExistsForLabelAndProperties(labelName, propertyNames)

      indexHintType match {
        case UsingAnyIndexType   => btreeExists || textExists
        case UsingBtreeIndexType => btreeExists
        case UsingTextIndexType  => textExists
      }
    }

    def relIndexHintFulfillable(labelOrRelType: LabelOrRelTypeName, properties: Seq[PropertyKeyName], indexHintType: UsingIndexHintType): Boolean = {
      val relTypeName = labelOrRelType.name
      val propertyNames = properties.map(_.name)

      def btreeExists = planContext.btreeIndexExistsForRelTypeAndProperties(relTypeName, propertyNames)
      def textExists = context.planningTextIndexesEnabled && planContext.textIndexExistsForRelTypeAndProperties(relTypeName, propertyNames)

      indexHintType match {
        case UsingAnyIndexType   => btreeExists || textExists
        case UsingBtreeIndexType => btreeExists
        case UsingTextIndexType  => textExists
      }
    }

    query.allHints.flatMap {
      // using index name:label(property1,property2)
      case UsingIndexHint(v, labelOrRelType, properties, _, indexHintType)
        if semanticTable.isNodeNoFail(v.name) && nodeIndexHintFulfillable(labelOrRelType, properties, indexHintType) =>
        None

      // using index name:relType(property1,property2)
      case UsingIndexHint(v, labelOrRelType, properties, _, indexHintType)
        if semanticTable.isRelationshipNoFail(v.name) && relIndexHintFulfillable(labelOrRelType, properties, indexHintType) =>
        None

      // no such index exists
      case hint: UsingIndexHint =>
        // Let's assume node type by default, in case we have no type information.
        val entityType = if (semanticTable.isRelationshipNoFail(hint.variable)) EntityType.RELATIONSHIP else EntityType.NODE
        Some(UnfulfillableIndexHint(hint, entityType))
      // don't care about other hints
      case _ => None
    }
  }

  private def findUnfulfillableJoinHints(query: PlannerQueryPart): Set[UsingJoinHint] = {
    query.allHints.collect {
      case hint: UsingJoinHint => hint
    }
  }
}
