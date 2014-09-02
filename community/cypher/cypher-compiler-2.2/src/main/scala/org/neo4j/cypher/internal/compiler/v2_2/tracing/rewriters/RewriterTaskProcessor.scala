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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.Rewriter

trait RewriterTaskProcessor extends (RewriterTask => Rewriter) {
  def sequenceName: String

  def apply(task: RewriterTask): Rewriter = task match {
    case RunConditions(name, conditions) =>
      (input: AnyRef) =>
        val result = conditions.toSeq.flatMap(cond => cond(input))
        if (result.isEmpty) {
          Some(input)
        } else {
          throw new RewritingConditionViolationException(name, result)
        }

    case RunRewriter(_, rewriter) =>
      rewriter
  }

  case class RewritingConditionViolationException(optName: Option[String], failures: Seq[RewriterConditionFailure])
    extends InternalException(buildMessage(sequenceName, optName, failures))

  private def buildMessage(sequenceName: String, optName: Option[String], failures: Seq[RewriterConditionFailure]) = {
    val name = optName.map(name => s"step '$name'").getOrElse("start of rewriting")
    val builder = new StringBuilder
    builder ++= s"Error during '$sequenceName' rewriting after $name. The following conditions where violated: "
    for (failure <- failures) {
      val name = failure.name
      for (problem <- failure.problems)
        builder ++= s"Condition '$name' violated. $problem"
    }
    builder.toString()
  }
}

case class DefaultRewriterTaskProcessor(sequenceName: String) extends RewriterTaskProcessor

case class TracingRewriterTaskProcessor(sequenceName: String, onlyWhenChanged: Boolean) extends RewriterTaskProcessor {
  override def apply(task: RewriterTask) = task match {
    case RunRewriter(name, rewriter) =>
      val innerRewriter = super.apply(task)
      (in: AnyRef) =>
        val result = innerRewriter(in)
        val always = !onlyWhenChanged
        if (always || in != result) // TODO: This test does not work. Investigate.
          print(s"$name ($sequenceName):\n\t$result\n")
        else
          print(s"$name ($sequenceName):\n\t--\n")
        result

    case _ =>
      super.apply(task)
  }
}

