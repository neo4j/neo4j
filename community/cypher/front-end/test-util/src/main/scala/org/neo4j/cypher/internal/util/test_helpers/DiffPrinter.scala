/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.test_helpers

import fansi.Color

object DiffPrinter {

  def println(
    before: String,
    after: String,
    harmonize: String => String = identity,
    isCondensed: Boolean = false
  ): Unit = {
    renderPrivate(before, after, harmonize, isCondensed).foreach(Predef.println)
  }

  def render(
    before: String,
    after: String,
    harmonize: String => String = identity,
    isCondensed: Boolean = false
  ): String = {
    renderPrivate(before, after, harmonize, isCondensed).mkString(System.lineSeparator())
  }

  private def renderPrivate(
    before: String,
    after: String,
    harmonize: String => String,
    isCondensed: Boolean
  ): Seq[String] = {
    val changeInstructions = StringLinesDiff(before, after, harmonize)
    val linesToRender =
      if (isCondensed) condense(changeInstructions)
      else wrap(changeInstructions)

    linesToRender.flatMap(render)
  }

  private def wrap(lines: Seq[StringLineChangeInstruction]): Seq[LineToRender] = lines.map(ci => RegularLine(ci))

  private def condense(changeInstructions: Seq[StringLineChangeInstruction]): Seq[LineToRender] = {
    sealed trait Segment
    case class KeepSegment(lines: Vector[KeepLine]) extends Segment
    case class ChangeSegment(lines: Vector[StringLineChangeInstruction]) extends Segment

    case class State(segments: Vector[Segment], currentKeepLines: Vector[KeepLine]) {
      def appendChange(changeInstruction: StringLineChangeInstruction): State =
        flushKeepLines match {
          case State(flushedSegments, _) => copy(
              segments = flushedSegments :+ ChangeSegment(Vector(changeInstruction)),
              currentKeepLines = Vector.empty
            )
        }

      def appendKeep(keepLine: KeepLine): State =
        copy(currentKeepLines = currentKeepLines :+ keepLine)

      def flushKeepLines: State =
        if (currentKeepLines.isEmpty) this
        else copy(segments = segments :+ KeepSegment(currentKeepLines), currentKeepLines = Vector.empty)
    }

    val segments = changeInstructions
      .foldLeft(State(Vector.empty, Vector.empty)) {
        case (state, keepLine: KeepLine) => state.appendKeep(keepLine)
        case (state, changeInstruction)  => state.appendChange(changeInstruction)
      }
      .flushKeepLines
      .segments

    segments.zipWithIndex.flatMap {
      case (KeepSegment(keepLines), index) =>
        if (keepLines.lengthCompare(10) < 0) wrap(keepLines)
        else if (index == 0 && index == segments.length - 1)
          wrap(keepLines.take(3)) ++ Seq(CondensedLine) ++ wrap(keepLines.takeRight(3))
        else if (index == 0) CondensedLine +: wrap(keepLines.takeRight(3))
        else if (index == segments.length - 1) wrap(keepLines.take(3)) :+ CondensedLine
        else wrap(keepLines.take(3)) ++ Seq(CondensedLine) ++ wrap(keepLines.takeRight(3))
      case (ChangeSegment(lines), _) => wrap(lines)
    }
  }

  private def render(lineToRender: LineToRender): Seq[String] = lineToRender match {
    case RegularLine(KeepLine(line))   => Seq(s"  $line")
    case RegularLine(AddLine(line))    => Seq(Color.Green(s"+ $line").render)
    case RegularLine(RemoveLine(line)) => Seq(Color.Red(s"- $line").render)
    case RegularLine(ReplaceLine(line, replacement)) =>
      Seq(
        Color.Red(s"- $line").render,
        Color.Green(s"+ $replacement").render
      )
    case CondensedLine => Seq("...")
  }

  sealed private trait LineToRender

  private case class RegularLine(changeInstruction: StringLineChangeInstruction) extends LineToRender
  private case object CondensedLine extends LineToRender
}
