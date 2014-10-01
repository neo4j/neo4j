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
package org.neo4j.cypher.internal.compiler.v2_2.perty.recipe

import org.neo4j.cypher.internal.compiler.v2_2.perty.step._
import org.neo4j.cypher.internal.compiler.v2_2.perty.{Doc, DocRecipe}

import scala.reflect.runtime.universe._

// DSL for the easy and well-formed construction of DocOps
//
// The DSL constructs and combines Appenders (functions from DocOps[T] => DocOps[T]).
//
// This approach removes otherwise annoying differences between DocOp[T] and Seq[DocOp[T]]
// as well as minimizes the amount of sequence concatenation.
//
// The DSL uses the stack' however stack depth shouldn't be a concern here as we support
// AddContent on this level and everything is flattened away by expandDocOps
//
class Pretty[T] extends LowPriorityPrettyImplicits[T] {
  def apply(appender: RecipeAppender[T]): DocRecipe[T] = appender(Seq.empty)

  def group(ops: RecipeAppender[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushGroupFrame +: ops(PopFrame +: append)
  }

  def nest(ops: RecipeAppender[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushNestFrame +: ops(PopFrame +: append)
  }

  def page(ops: RecipeAppender[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushPageFrame +: ops(PopFrame +: append)
  }

  def break = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddBreak +: append
  }

  def breakWith(text: String) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddBreak(Some(text)) +: append
  }

  def empty = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = append
  }

  def doc(doc: Doc) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddDoc(doc) +: append
  }

  implicit def text(text: String) = new RecipeAppender[T] {
      def apply(append: DocRecipe[T]) = AddText(text) +: append
    }

  implicit def liftDocRecipe(opts: DocRecipe[T]) = Some(opts)
}

protected class LowPriorityPrettyImplicits[T] {
  implicit def pretty[S <: T : TypeTag](content: => S) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddPretty(content) +: append
  }
}

object Pretty extends Pretty[Any]


