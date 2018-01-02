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
package org.neo4j.cypher.internal.frontend.v2_3.perty.recipe

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.toStringDocGen
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.DocRecipe.strategyExpander
import org.neo4j.cypher.internal.frontend.v2_3.perty.step._

import scala.reflect.runtime.universe.TypeTag
import scala.annotation.tailrec

// Helper for evaluating a PrintableDocRecipe into a Doc
//
// The resulting Doc is sanitized on the fly (removes unneeded wrappings, nils, and conses)
//
// Evaluation executes on the heap to avoid blowing up the JVM stack
//
case object PrintableDocRecipe {

  case class evalUsingStrategy[T : TypeTag, S >: T : TypeTag](docGen: DocGenStrategy[S] = toStringDocGen) extends (DocRecipe[T] => Doc) {
    def apply(recipe: DocRecipe[T]): Doc = {
      val expander = strategyExpander[T, S](docGen)
      val expanded = expander.expandForPrinting(recipe)
      val printable = eval(expanded)
      printable
    }
  }

  case object eval extends (PrintableDocRecipe => Doc) {

    def apply(ops: Seq[PrintableDocStep]): Doc = ops match {
      case Seq() => NilDoc
      case _     => convert(ops :+ PopFrame, Seq(FlatTopLevelDocFrame(Seq.empty)))
    }

    @tailrec
    private def convert(input: Seq[DocStep[String]], frames: Seq[DocFrame] = Seq.empty): Doc = (input, frames) match {
      case (Seq(AddText(text: String), tail@_*), Seq(frame, tailFrames@_*)) =>
        convert(tail, (frame :+ TextDoc(text)) +: tailFrames)

      case (Seq(AddBreak(Some(breakWith: String)), tail@_*), Seq(frame, tailFrames@_*)) =>
        convert(tail, (frame :+ BreakWith(breakWith)) +: tailFrames)

      case (Seq(AddBreak(None), tail@_*), Seq(frame, tailFrames@_*)) =>
        convert(tail, (frame :+ BreakDoc) +: tailFrames)

      case (Seq(AddNoBreak, tail@_*), Seq(frame, tailFrames@_*)) =>
        convert(tail, (frame :+ NoBreak) +: tailFrames)

      case (Seq(AddDoc(doc: Doc), tail@_*), Seq(frame, tailFrames@_*)) =>
        doc match {
          case NilDoc => convert(tail, frames)
          case _ => convert(tail, (frame :+ doc) +: tailFrames)
        }

      case (Seq(op: PushFrame, tail@_*), tailFrames) =>
        val newFrame: DocFrame = op match {
          case PushGroupFrame        => GroupDocFrame.empty
          case PushNestFrame(None)   => NestDocFrame.empty
          case PushNestFrame(indent) => NestDocFrame(indent, Seq.empty)
          case PushPageFrame         => PageDocFrame.empty
        }
        convert(tail, newFrame +: tailFrames)

      case (Seq(PopFrame, tail@_*), Seq(curFrame, nextFrame, tailFrames@_*)) =>
        curFrame.asDoc match {
          case NilDoc => convert(tail, nextFrame +: tailFrames)
          case doc    => convert(tail, (nextFrame :+ doc) +: tailFrames)
        }

      case (Seq(PopFrame), Seq(frame)) =>
        frame.asDoc

      case _ =>
        throw new IllegalArgumentException(s"Unbalanced sequence of DocOps input: $input, frames: $frames")
    }

    private sealed abstract class DocFrame {
      val docs: Seq[Doc]

      def :+(doc: Doc): DocFrame

      def content = docs match {
        case Seq()    => NilDoc
        case Seq(doc) => doc
        case _        =>  docs.foldRight[Doc](NilDoc)(ConsDoc)
      }

      def asDoc: Doc =
        content match {
          case NilDoc =>
            NilDoc

          case leafDoc: ValueDoc =>
            leafDoc

          case doc =>
            docFromContent(doc)
        }

      protected def docFromContent(doc: Doc): Doc
    }

    sealed private case class FlatTopLevelDocFrame(docs: Seq[Doc]) extends DocFrame {

      def :+(doc: Doc): DocFrame = copy(docs = docs :+ doc)

      protected def docFromContent(doc: Doc): Doc = doc
    }

    sealed private case class GroupDocFrame(docs: Seq[Doc]) extends DocFrame {

      def :+(doc: Doc): DocFrame = copy(docs = docs :+ doc)

      protected def docFromContent(doc: Doc): Doc = doc match {
        case inner: GroupDoc => inner
        case _               => GroupDoc(doc)
      }
    }

    private object GroupDocFrame {
      val empty = GroupDocFrame(Seq.empty)
    }

    sealed private case class NestDocFrame(indent: Option[Int] = None, docs: Seq[Doc]) extends DocFrame {

      def :+(doc: Doc): DocFrame = copy(docs = docs :+ doc)

      protected def docFromContent(doc: Doc): Doc = indent match {

        case None =>
          NestDoc(doc)

        case Some(outerIndent) =>
          doc match {
            case NestWith(innerIndent, innerDoc) =>
              NestWith(outerIndent + innerIndent, innerDoc)
            case _ =>
              NestWith(outerIndent, doc)
          }
      }
    }

    private object NestDocFrame {
      val empty = NestDocFrame(None, Seq.empty)
    }

    sealed private case class PageDocFrame(docs: Seq[Doc]) extends DocFrame {

      def :+(doc: Doc): DocFrame = copy(docs = docs :+ doc)

      protected def docFromContent(doc: Doc): Doc = doc match {
        case inner: PageDoc => inner
        case _              => PageDoc(doc)
      }
    }

    private object PageDocFrame {
      val empty = PageDocFrame(Seq.empty)
    }
  }
}
