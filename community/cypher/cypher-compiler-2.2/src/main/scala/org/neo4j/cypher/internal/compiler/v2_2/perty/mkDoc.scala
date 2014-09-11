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
package org.neo4j.cypher.internal.compiler.v2_2.perty

import scala.annotation.tailrec

case object mkDoc extends (Seq[DocOp[String]] => Doc) {

  def apply(ops: Seq[DocOp[String]]): Doc =
    ops match {
      case Seq(_: PushFrame, _*) => convert(ops)
      case Seq()                 => NilDoc
      case _                     => convert(PushFrameGroup +: ops :+ PopFrame)
    }

  @tailrec
  private def convert(input: Seq[DocOp[String]], docs: Seq[DocFrame] = Seq.empty): Doc = (input, docs) match {
    case (Seq(AddContent(v: String), tl@_*), Seq(frame, tlFrames@_*)) =>
      convert(tl, (frame :+ TextDoc(v)) +: tlFrames)

    case (Seq(AddBreak(Some(v: String)), tl@_*), Seq(frame, tlFrames@_*)) =>
      convert(tl, (frame :+ BreakWith(v)) +: tlFrames)

    case (Seq(AddBreak(None), tl@_*), Seq(frame, tlFrames@_*)) =>
      convert(tl, (frame :+ BreakDoc) +: tlFrames)

    case (Seq(AddNoBreak, tl@_*), Seq(frame, tlFrames@_*)) =>
      convert(tl, (frame :+ NoBreak) +: tlFrames)

    case (Seq(PopFrame, tl@_*), Seq(curFrame, nextFrame, tlFrames@_*)) =>
      convert(tl, (nextFrame :+ curFrame.asDoc) +: tlFrames)

    case (Seq(PopFrame), Seq(frame)) =>
      frame.asDoc
  }

  case class DocFrame(start: PushFrame, docs: Seq[Doc] = Seq.empty) {
    def :+(doc: Doc) = copy(docs = docs :+ doc)

    def content = Doc.list(docs)

    def asDoc: Doc = start match {
      case PushFrameGroup              => GroupDoc(content)
      case PushFramePage               => PageDoc(content)
      case PushFrameNest(None)         => NestDoc(content)
      case PushFrameNest(Some(indent)) => NestWith(indent, content)
    }
  }
}
