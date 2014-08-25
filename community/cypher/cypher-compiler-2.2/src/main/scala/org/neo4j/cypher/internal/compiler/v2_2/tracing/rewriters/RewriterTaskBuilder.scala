/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters

import scala.annotation.tailrec

case object RewriterTaskBuilder {

  def apply(steps: Seq[RewriterStep]) = buildTasks(Set.empty, None, steps, Seq.empty)

  @tailrec
  private def buildTasks(conditions: Set[RewriterCondition], previousName: Option[String], steps: Seq[RewriterStep],
                         tasks: Seq[RewriterTask]): Seq[RewriterTask] = steps match {
    case Seq(hd, tl@_*) =>
      hd match {
        case ApplyRewriter(name, rewriter) =>
          val newTasks = withEnabledConditions(tasks, previousName, conditions) :+ RunRewriter(name, rewriter)
          buildTasks(conditions, Some(name), tl, newTasks)
        case EnableRewriterCondition(cond) =>
          buildTasks(conditions + cond, previousName, tl, tasks)
        case DisableRewriterCondition(cond) =>
          buildTasks(conditions - cond, previousName, tl, tasks)
        case EmptyRewriterStep =>
          buildTasks(conditions, previousName, tl, tasks)
      }

    case _ =>
      withEnabledConditions(tasks, previousName, conditions)
  }

  private def withEnabledConditions(tasks: Seq[RewriterTask], previousName: Option[String], conditions: Set[RewriterCondition]) =
    if (conditions.isEmpty) {
      tasks
    } else {
      tasks :+ RunConditions(previousName, conditions)
    }
}
