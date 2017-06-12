/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting

import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, Rewriter}

trait RewriterTaskProcessor extends (RewriterTask => Rewriter) {
  def sequenceName: String

  def apply(task: RewriterTask): Rewriter = task match {
    case RunConditions(name, conditions) => RunConditionRewriter(sequenceName, name, conditions)
    case RunRewriter(_, rewriter) => rewriter
  }
}

case class RunConditionRewriter(sequenceName: String, name: Option[String], conditions: Set[RewriterCondition]) extends Rewriter {
  def apply(input: AnyRef): AnyRef = {
    val failures = conditions.toIndexedSeq.flatMap(condition => condition(input))
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
