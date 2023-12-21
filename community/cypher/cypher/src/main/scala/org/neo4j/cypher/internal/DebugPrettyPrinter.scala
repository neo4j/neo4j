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
package org.neo4j.cypher.internal

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.Doc
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.any
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.line
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.link
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.list
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.nest
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.pretty
import org.bitbucket.inkytonik.kiama.output.PrettyPrinter.text
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException

trait DebugPrettyPrinter {
  val PRINT_QUERY_TEXT = true
  val PRINT_LOGICAL_PLAN = true
  val PRINT_REWRITTEN_LOGICAL_PLAN = true
  val PRINT_PIPELINE_INFO = true
  val PRINT_FAILURE_STACK_TRACE = true

  protected def printPlanInfo(logicalQuery: LogicalQuery): Unit = {
    println(s"\n========================================================================")
    if (PRINT_QUERY_TEXT)
      println(s"\u001b[32m[QUERY]\n\n${logicalQuery.queryText}") // Green
    if (PRINT_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[LOGICAL PLAN]\n") // Magenta
      println(logicalQuery.logicalPlan)
    }
    println("\u001b[0m") // Reset
  }

  protected def printRewrittenPlanInfo(logicalPlan: LogicalPlan): Unit = {
    if (PRINT_REWRITTEN_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[REWRITTEN LOGICAL PLAN]\n") // Magenta
      println(logicalPlan)
    }
    println("\u001b[0m") // Reset
  }

  protected def printPipe(slotConfigurations: SlotConfigurations, pipe: Pipe = null): Unit = {
    if (PRINT_PIPELINE_INFO) {
      println(s"\n\u001b[36m[SLOT CONFIGURATIONS]\n") // Cyan
      prettyPrintPipelines(slotConfigurations)
      if (pipe != null) {
        println(s"\n\u001b[34m[PIPE INFO]\n") // Blue
        prettyPrintPipe(pipe)
      }
    }
    println("\u001b[0m") // Reset
  }

  protected def printFailureStackTrace(e: CypherException): Unit = {
    if (PRINT_FAILURE_STACK_TRACE) {
      println("------------------------------------------------")
      println("<<< Slotted failed because:\u001b[31m") // Red
      e.printStackTrace(System.out)
      println("\u001b[0m>>>") // Reset
      println("------------------------------------------------")
    }
  }

  private def prettyPrintLogicalPlan(plan: LogicalPlan): Unit = {
    val planAnsiPre = "\u001b[1m\u001b[35m" // Bold on + magenta
    val planAnsiPost = "\u001b[21m\u001b[35m" // Restore to bold off + magenta
    def prettyPlanName(plan: LogicalPlan) = s"$planAnsiPre${plan.productPrefix}$planAnsiPost"
    def prettyId(id: Id) = s"\u001b[4m\u001b[35m${id}\u001b[24m\u001b[35m" // Underlined + magenta

    def show(v: Any): Doc =
      link(
        v.asInstanceOf[AnyRef],
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
                  case _         => true
                }
                list(plan.id :: otherElements, prettyPlanName(plan), show) <>
                  line <> show(lhs)

              case (Some(lhs), Some(rhs)) =>
                val otherElements: List[Any] = plan.productIterator.toList.filter {
                  case e: AnyRef => (e ne lhs) && (e ne rhs)
                  case _         => true
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
    val transformedPipelines = pipelines.iterator.foldLeft(Seq.empty[(Int, Any)]) {
      case (acc, (k: Id, v)) => acc :+ (k.x -> v)
    }.sortBy {
      case (k, _) => k
    }
    val prettyDoc = pretty(any(transformedPipelines), w = 120)
    println(prettyDoc.layout)
  }

  protected def prettyPrintPipe(pipe: Pipe): Unit = {
    val prettyDoc = pretty(any(pipe), w = 120)
    println(prettyDoc.layout)
  }
}
