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

import org.neo4j.cypher.internal.frontend.v2_3.perty.step.{PrintableDocStep, AddPretty, DocStep}
import org.neo4j.cypher.internal.frontend.v2_3.perty._

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

case object DocRecipe {

  // Removes any occurrence of AddContent from the input by repeatedly calling the given extractor
  // on it. If calling the extractor does not succeed, fails.
  //
  case class strategyExpander[T : TypeTag, S >: T : TypeTag](extractor: DocGenStrategy[S]) {
    val tpe = typeOf[S]

    case object expandForPrinting extends (DocRecipe[T] => PrintableDocRecipe) {
      def apply(input: DocRecipe[T]) = {
        @tailrec
        def expand(remaining: DocRecipe[Any], result: PrintableDocRecipe = Seq.empty): PrintableDocRecipe = remaining match {
          case Seq(hd: AddPretty[_], tl@_*) =>
            throw new IllegalArgumentException(
              s"Cannot expand value of type ${hd.tpe}, it is not within the extractor's supported type bound <:< $tpe (extractor: $extractor)"
            )

          case Seq(hd: PrintableDocStep, tl@_*) =>
            expand(tl, result :+ hd)

          case Seq() =>
            result
        }

        expand(expandForQuoting(input))
      }
    }

    case object expandForQuoting extends (DocRecipe[T] => DocRecipe[T]) {
      def apply(input: DocRecipe[T]) = {
        @tailrec
        def expand(remaining: DocRecipe[Any], result: DocRecipe[T] = Seq.empty): DocRecipe[T] = remaining match {
          case Seq(hd: AddPretty[_], tl@_*) if hd.tpe <:< tpe =>
            formatErrors {
              hd.asInstanceOf[AddPretty[_ <: T]](extractor)
            } match {
              case Some(recipe) =>
                expand(recipe ++ tl, result)

              case None =>
                throw new IllegalArgumentException(
                  s"Extractor failed to expand value of type: ${hd.tpe} (extractor: $extractor)"
                )
            }

          case Seq(hd: PrintableDocStep, tl@_*) =>
            expand(tl, result :+ hd)

          case Seq() =>
            result
        }

        expand(input)
      }
    }
  }

  case object IsEmpty extends (DocRecipe[Any] => Boolean) {
    override def apply(recipe: DocRecipe[Any]): Boolean =
      recipe.isEmpty
  }
}
