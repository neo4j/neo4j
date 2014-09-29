package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.compiler.v2_2.perty.{DocOps, DocGen}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

case class expandDocOps[T : TypeTag](extractor: DocGen[T]) extends (Seq[DocOp[Any]] => Seq[BaseDocOp]) {
  def apply(input: Seq[DocOp[Any]]) = {
    @tailrec
    def expand(remaining: Seq[DocOp[Any]], result: Seq[BaseDocOp]): Seq[BaseDocOp] = remaining match {
      case Seq(hd: AddContent[_], tl@_*) if hd.tag.tpe <:< typeOf[T] =>
        extractContent(hd.asInstanceOf[AddContent[T]]) match {
          case Some(replacement) =>
            expand(replacement ++ tl, result)

          case None =>
            throw new IllegalArgumentException(s"Extractor failed to handle value of type: ${hd.tag.tpe}")
        }

      case Seq(hd: AddContent[_], tl@_*) =>
        throw new IllegalArgumentException(s"Extracted value out of type bounds: ${hd.tag.tpe} >:> ${typeOf[T]}")

      case Seq(hd: BaseDocOp, tl@_*) =>
        expand(tl, result :+ hd)

      case Seq() =>
        result
    }

    expand(input, Seq.empty)
  }

  def extractContent(hd: AddContent[T]): Option[DocOps[Any]] =
    formatErrors { hd(extractor) }
}
