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
package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.compiler.v2_2.perty.{Doc, DocOps, DocGen}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

// Removes any occurrence of AddContent from the input by repeatedly calling the given extractor
// on it. If calling the extractor does not succeed, fails.
//
case class expandDocOps[T : TypeTag](extractor: DocGen[T]) extends (Seq[DocOp[Any]] => Seq[BaseDocOp]) {
  def apply(input: Seq[DocOp[Any]]) = {
    @tailrec
    def expand(remaining: Seq[DocOp[Any]], result: Seq[BaseDocOp]): Seq[BaseDocOp] = remaining match {
      case Seq(hd: AddPretty[_], tl@_*) if hd.tag.tpe <:< typeOf[T] =>
        formatErrors { extractContent(hd.asInstanceOf[AddPretty[_ <: T]]) } match {
          case Some(replacement) =>
            expand(replacement ++ tl, result)

          case None =>
            throw new IllegalArgumentException(
              s"Extractor failed to expand value of type: ${hd.tag.tpe} (extractor: $extractor)"
            )
        }

      case Seq(hd: AddPretty[_], tl@_*) =>
        throw new IllegalArgumentException(
          s"Cannot expand value of type ${typeOf[T]} not within the extractor's supported type bound <:< ${hd.tag.tpe} (extractor: $extractor)"
        )

      case Seq(hd: BaseDocOp, tl@_*) =>
        expand(tl, result :+ hd)

      case Seq() =>
        result
    }

    expand(input, Seq.empty)
  }

  def extractContent[U <: T](hd: AddPretty[U]): Option[DocOps[Any]] = hd(extractor)
}
