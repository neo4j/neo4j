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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2._

import scala.annotation.tailrec

sealed trait RewriterStep
final case class NamedRewriter(name: String, rewriter: Rewriter) extends RewriterStep
final case class EnableRewriterCondition(cond: RewriterCondition) extends RewriterStep
final case class DisableRewriterCondition(cond: RewriterCondition) extends RewriterStep
case object NoRewriter extends RewriterStep

object RewriterStep {
  implicit def namedProductRewriter(p: Product with Rewriter) = NamedRewriter(p.productPrefix, p)
  implicit def productRewriterCondition(p: Product with (AnyRef => Seq[String])) =  RewriterCondition(p.productPrefix, p)
  def enableCondition(p: Product with (AnyRef => Seq[String])) = EnableRewriterCondition(p)
  def disableCondition(p: Product with (AnyRef => Seq[String])) = DisableRewriterCondition(p)
}

final case class RewriterCondition(name: String, condition: AnyRef => Seq[String]) {
  def apply(input: AnyRef): Option[RewriterConditionFailure] = {
    val problems = condition(input)
    if (problems.isEmpty) None else Some(RewriterConditionFailure(name, problems))
  }
}

case class RewriterConditionFailure(name: String, problems: Seq[String])

case class RewriterStepSequencer(sequenceName: String, rewriterWrapper: NamedRewriter => Rewriter = _.rewriter) {
  def apply(steps: RewriterStep*): Rewriter = inSequence(apply(steps.toList): _*)

  def apply(steps: List[RewriterStep]): Seq[Rewriter] = {
    val tasks = buildTasks(Set.empty, None, steps, Seq.empty)
    val rewriters = tasks.map(taskRewriter)
    rewriters
  }

  @tailrec
  private def buildTasks(
    currentConditions: Set[RewriterCondition],
    currentName: Option[String],
    input: List[RewriterStep],
    output: Seq[Either[(Option[String], Set[RewriterCondition]), NamedRewriter]]): Seq[Either[(Option[String], Set[RewriterCondition]), NamedRewriter]] =
      input match {
        case hd :: tl =>
          hd match {
            case named: NamedRewriter =>
              buildTasks(currentConditions, Some(named.name), tl, outputWithConditions(output, currentName, currentConditions) :+ Right(named))
            case EnableRewriterCondition(cond) =>
              buildTasks(currentConditions + cond, currentName, tl, output)
            case DisableRewriterCondition(cond) =>
              buildTasks(currentConditions - cond, currentName, tl, output)
            case NoRewriter =>
              buildTasks(currentConditions, currentName, tl, output)
          }
        case _ =>
          outputWithConditions(output, currentName, currentConditions)
      }

  private def outputWithConditions(output: Seq[Either[(Option[String], Set[RewriterCondition]), NamedRewriter]], name: Option[String], conditions: Set[RewriterCondition]) =
    if (conditions.isEmpty) output else output :+ Left((name, conditions))

  private def taskRewriter(rewritersOrConditions: Either[(Option[String], Set[RewriterCondition]), NamedRewriter]): Rewriter =
    rewritersOrConditions match {
      case Left((name, conditions)) =>
        (input: AnyRef) =>
          val result: Seq[RewriterConditionFailure] = conditions.toSeq.flatMap(cond => cond(input))
          if (result.isEmpty) Some(input) else throwConditionViolatedException(name, result)

      case Right(named: NamedRewriter) =>
        rewriterWrapper(named)
    }

  private def throwConditionViolatedException(optName: Option[String], failures: Seq[RewriterConditionFailure]) = {
    val name = optName.map(name => s"step '$name'").getOrElse("start of rewriting")
    val builder = new StringBuilder
    builder ++= s"Error during '$sequenceName' rewriting after $name. The following conditions where violated: "
    for (failure <- failures) {
      val name = failure.name
      for (problem <- failure.problems)
        builder ++= s"Condition '$name' violated. $problem"
    }
    throw new InternalException(builder.toString())
  }
}

case object TracingRewriterStepSequencer {
  def apply(sequenceName: String) = RewriterStepSequencer(
    sequenceName = sequenceName,
    rewriterWrapper = (namedRewriter) => {
      (in: AnyRef) => {
        val result = namedRewriter.rewriter(in)
        print(s"${namedRewriter.name} ($sequenceName):\n\t$result\n")
        result
      }
    }
  )
}
