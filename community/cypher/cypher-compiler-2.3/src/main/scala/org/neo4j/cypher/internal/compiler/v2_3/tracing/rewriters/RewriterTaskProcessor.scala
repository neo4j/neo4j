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

import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, Rewriter}

trait RewriterTaskProcessor extends (RewriterTask => Rewriter) {
  def sequenceName: String

  def apply(task: RewriterTask): Rewriter = task match {
    case RunConditions(name, conditions) => RunConditionRewriter(sequenceName, name, conditions)
    case RunRewriter(_, rewriter) => rewriter
  }
}

case class RunConditionRewriter(sequenceName: String, name: Option[String], conditions: Set[RewriterCondition]) extends Rewriter {
  def apply(input: AnyRef): AnyRef = {
    val failures = conditions.toSeq.flatMap(condition => condition(input))
    if (failures.isEmpty) {
      input
    } else {
      throw new RewritingConditionViolationException(name, failures)
    }
  }

  case class RewritingConditionViolationException(optName: Option[String], failures: Seq[RewriterConditionFailure])
    extends InternalException(buildMessage(sequenceName, optName, failures))

  private def buildMessage(sequenceName: String, optName: Option[String], failures: Seq[RewriterConditionFailure]) = {
    val name = optName.map(name => s"step '$name'").getOrElse("start of rewriting")
    val builder = new StringBuilder
    builder ++= s"Error during '$sequenceName' rewriting after $name. The following conditions where violated: \n"
    for (failure <- failures ;
         problem <- failure.problems) {
      builder ++= s"Condition '${failure.name}' violated. $problem\n"
    }
    builder.toString()
  }
}

case class DefaultRewriterTaskProcessor(sequenceName: String) extends RewriterTaskProcessor

case class TracingRewriterTaskProcessor(sequenceName: String, onlyWhenChanged: Boolean) extends RewriterTaskProcessor {

  override def apply(task: RewriterTask) = task match {
    case RunRewriter(name, _) =>
      val innerRewriter = super.apply(task)
      (in) =>
        val result = innerRewriter(in)
        if (!onlyWhenChanged || in != result) {
//          val resultDoc = pprintToDoc[AnyRef, Any](Result(result))(ResultHandler.docGen)
//          val resultString = printCommandsToString(DocFormatters.defaultFormatter(resultDoc))
          val resultString = result.toString
          Console.print(s"*** $name ($sequenceName):$resultString\n")
        } else {
          Console.print(s"*** $name ($sequenceName):\n--\n")
        }
        result

    case _ =>
      super.apply(task)
  }
}
//
//object TracingRewriterTaskProcessor {
//  import Pretty._
//
//  object ResultHandler extends CustomDocHandler[Any] {
//    def docGen = resultDocGen orElse InternalDocHandler.docGen
//  }
//
//  object resultDocGen extends CustomDocGen[Any] {
//    def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
//      case Result(result) => Pretty(page(nestWith(indent = 2, group(break :: group(pretty(result))))))
//      case _              => None
//    }
//  }
//
//  case class Result(v: Any)
//}
