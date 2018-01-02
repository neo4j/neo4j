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

import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.DocRecipe.IsEmpty
import org.neo4j.cypher.internal.frontend.v2_3.perty.{DocLiteral, Doc, DocGenStrategy, DocRecipe}
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.toStringDocGen
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{pprintToDoc, pprint}
import org.neo4j.cypher.internal.frontend.v2_3.perty.step.{AddDoc, DocStep, AddPretty, AddBreak}

import scala.reflect.runtime.universe.TypeTag

// RecipeAppenders are DocRecipe[T] producers that take an
// additional argument that gets appended to the produces
// representation
abstract class RecipeAppender[T : TypeTag] extends (DocRecipe[T] => DocRecipe[T]) {
  self =>

  def ::(hd: RecipeAppender[T]): RecipeAppender[T] = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = hd(self(append))
  }

  def :/:(hd: RecipeAppender[T]): RecipeAppender[T] = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = hd(AddBreak +: self(append))
  }

  def :?:(hd: RecipeAppender[T]): RecipeAppender[T] = {
    // Drop empty rhs argument
    test(IsEmpty)(_ => hd)(doc => RecipeAppender(hd(doc)))
  }

  def :/?:(hd: RecipeAppender[T]): RecipeAppender[T] = {
    // Drop any empty argument, or if both are non-empty, insert break
    hd.test(IsEmpty)(_ => self)(hdDoc => test(IsEmpty)(_ => RecipeAppender(hdDoc))(_ => RecipeAppender(hdDoc) :/: self))
  }

  def test(cond: DocRecipe[T] => Boolean = IsEmpty)
          (ifEmpty: DocRecipe[T] => RecipeAppender[T])
          (ifNonEmpty: DocRecipe[T] => RecipeAppender[T]): RecipeAppender[T] = {
    val recipe = self(Seq.empty)
    if (cond(recipe)) ifEmpty(recipe) else ifNonEmpty(recipe)
  }

  override def toString() = s"Doc(asDocString())"

  def asDoc(docGen: DocGenStrategy[Any] = toStringDocGen): Doc =
    PrintableDocRecipe.evalUsingStrategy[T, Any](docGen).apply(self(Seq.empty))

  def asDocString(docGen: DocGenStrategy[Any] = toStringDocGen) =
    DocLiteral(asDoc(docGen)).toString
}

object RecipeAppender {
  def apply[T : TypeTag](doc: DocRecipe[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = doc ++ append
  }
}
