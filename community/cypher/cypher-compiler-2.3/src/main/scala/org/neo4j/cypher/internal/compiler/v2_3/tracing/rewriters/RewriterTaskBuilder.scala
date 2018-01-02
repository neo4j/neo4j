/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.Rewriter

case object RewriterTaskBuilder {

  private case class State(conditions: Set[RewriterCondition] = Set.empty,
                           previousName: Option[String] = None,
                           tasks: Seq[RewriterTask] = Seq.empty) {
    def +(name: String, rewriter: Rewriter) =
      copy(previousName = Some(name), tasks = allTasks :+ RunRewriter(name, rewriter))
    def +(condition: RewriterCondition) = copy(conditions = conditions + condition)
    def -(condition: RewriterCondition) = copy(conditions = conditions - condition)
    def allTasks = if (conditions.isEmpty) tasks else tasks :+ RunConditions(previousName, conditions)
  }

  def apply(steps: Seq[RewriterStep]): Seq[RewriterTask] = steps.foldLeft(State()) {
    case (state, ApplyRewriter(name, rewriter)) => state +(name, rewriter)
    case (state, EnableRewriterCondition(condition)) => state + condition
    case (state, DisableRewriterCondition(condition)) => state - condition
  }.allTasks
}
