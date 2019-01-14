/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v3_4.phases.LogicalPlanState
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.{CypherException, InternalException}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

trait DebugPrettyPrinter {
  val PRINT_QUERY_TEXT = true
  val PRINT_LOGICAL_PLAN = true
  val PRINT_REWRITTEN_LOGICAL_PLAN = true
  val PRINT_PIPELINE_INFO = true
  val PRINT_FAILURE_STACK_TRACE = true

  protected def printPlanInfo(lpState: LogicalPlanState) = {
    println(s"\n========================================================================")
    if (PRINT_QUERY_TEXT)
      println(s"\u001b[32m[QUERY]\n\n${lpState.queryText}") // Green
    if (PRINT_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[LOGICAL PLAN]\n") // Magenta
      prettyPrintLogicalPlan(lpState.logicalPlan)
    }
    println("\u001b[30m")
  }

  protected def printRewrittenPlanInfo(logicalPlan: LogicalPlan) = {
    if (PRINT_REWRITTEN_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[REWRITTEN LOGICAL PLAN]\n") // Magenta
      prettyPrintLogicalPlan(logicalPlan)
    }
    println("\u001b[30m")
  }

  protected def printPipeInfo(slotConfigurations: SlotConfigurations, pipeInfo: PipeInfo) = {
    if (PRINT_PIPELINE_INFO) {
      println(s"\n\u001b[36m[SLOT CONFIGURATIONS]\n") // Cyan
      prettyPrintPipelines(slotConfigurations)
      println(s"\n\u001b[34m[PIPE INFO]\n") // Blue
      prettyPrintPipeInfo(pipeInfo)
    }
    println("\u001b[30m")
  }

  protected def printFailureStackTrace(e: CypherException) = {
    if (PRINT_FAILURE_STACK_TRACE) {
      println("------------------------------------------------")
      println("<<< Slotted failed because:\u001b[31m") // Red
      e.printStackTrace(System.out)
      println("\u001b[30m>>>")
      println("------------------------------------------------")
    }
  }

  private def prettyPrintLogicalPlan(plan: LogicalPlan): Unit = {
    val planAnsiPre = "\u001b[1m\u001b[35m" // Bold on + magenta
    val planAnsiPost = "\u001b[21m\u001b[35m" // Restore to bold off + magenta
    def prettyPlanName(plan: LogicalPlan) = s"$planAnsiPre${plan.productPrefix}$planAnsiPost"
    def prettyId(id: Id) = s"\u001b[4m\u001b[35m${id}\u001b[24m\u001b[35m" // Underlined + magenta

    def show(v: Any): Doc =
      link(v.asInstanceOf[AnyRef],
        v match {
          case id: Id =>
            text(prettyId(id))

          case plan: LogicalPlan =>
            (plan.lhs, plan.rhs) match {
              case (None, None) =>
                val elements = plan.productIterator.toList
                list(plan.id :: elements, prettyPlanName(plan), show)

              case (Some(lhs), None) =>
                val otherElements: List[Any] = plan.productIterator.toList.filter {
                  case e: AnyRef => e ne lhs
                  case _ => true
                }
                list(plan.id :: otherElements, prettyPlanName(plan), show) <>
                  line <> show(lhs)

              case (Some(lhs), Some(rhs)) =>
                val otherElements: List[Any] = plan.productIterator.toList.filter {
                  case e: AnyRef => (e ne lhs) && (e ne rhs)
                  case _ => true
                }
                val lhsDoc = "[LHS]" <> line <> nest(show(lhs), 2)
                val rhsDoc = s"[RHS of ${plan.getClass.getSimpleName} (${plan.id})]" <> line <> nest(show(rhs), 2)
                list(plan.id :: otherElements, prettyPlanName(plan), show) <>
                  line <> nest(lhsDoc, 2) <>
                  line <> nest(rhsDoc, 2)

              case _ =>
                throw new InternalException("Invalid logical plan structure")
            }

          case _ =>
            any(v)
        }
      )

    val prettyDoc = pretty(show(plan), w = 120)
    println(prettyDoc.layout)
  }

  protected def prettyPrintPipelines(pipelines: SlotConfigurations): Unit = {
    val transformedPipelines = pipelines.iterator.foldLeft(Seq.empty[Any]) {
      case (acc, (k: Id, v)) => acc :+ (k.x -> v)
    }.sortBy { case (k: Int, _) => k }
    val prettyDoc = pretty(any(transformedPipelines), w = 120)
    println(prettyDoc.layout)
  }

  protected def prettyPrintPipeInfo(pipeInfo: PipeInfo): Unit = {
    val prettyDoc = pretty(any(pipeInfo), w = 120)
    println(prettyDoc.layout)
  }
}
