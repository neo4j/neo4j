/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2.Rewriter

import scala.annotation.tailrec

case object RewriterTaskBuilder {

  def apply(steps: Seq[RewriterStep]) = State()(steps)

  final private case class State(
    conditions: Set[RewriterCondition] = Set.empty,
    previousName: Option[String] = None,
    tasks: Seq[RewriterTask] = Seq.empty
  ) {

    self =>
    @tailrec
    def apply(steps: Seq[RewriterStep]): Seq[RewriterTask] = steps match {
      case Seq(hd, tl @_*) =>
        hd match {
          case ApplyRewriter(name, rewriter) =>
            withConditionsAppended.withRewriterAppended(name, rewriter).apply(tl)

          case EnableRewriterCondition(cond) =>
            copy(conditions + cond)(tl)

          case DisableRewriterCondition(cond) =>
            copy(conditions - cond)(tl)

          case EmptyRewriterStep =>
            self(tl)
        }

      case _ =>
        withConditionsAppended.tasks
    }

    private def withRewriterAppended(name: String, rewriter: Rewriter) =
      copy(previousName = Some(name), tasks = tasks :+ RunRewriter(name, rewriter))

    private def withConditionsAppended =
      if (conditions.isEmpty) self else copy(tasks = tasks :+ RunConditions(previousName, conditions))
  }
}
