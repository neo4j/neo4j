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
import org.neo4j.cypher.internal.compiler.v2_2.perty.{BreakingDoc, DocLiteral, Doc, DocRecipe}

import scala.reflect.runtime.universe._

// DSL for the easy and well-formed construction of DocRecipes
//
// DocRecipes are flat representations of Docs that are suitable for
// composition without hitting stack-depth problems. In particular,
// recipes can contain abstract content that needs to be replaced
// with actually printable content (PrintableDocSteps instead
// of regular DocSteps).
//
// However, building up DocRecipes is not as straightforward
// as using Docs directly and does not ensure structural well-formedness.
//
// This gap is filled by Pretty. Pretty is a DSL for building up
// a tree of RecipeAppenders.  Applying Pretty to a RecipeAppender
// yields a DocRecipe, a flat representation of the tree described
// by the DSL.
//
// Usually Pretty will only be used to render one "layer" (parent node
// without it's children) of pretty printing and thus building a tree of
// RecipeAppenders does not create a stack-depth problem. Multiple layers of
// pretty-printing can then be composed safely by replacing abstract content
// with DocRecipes from other layers. This is exactly what happens
// in DocRecipe.strategyExpander.
//
// Additionally, Pretty contains quite a few helper methods for easing
// the construction of DocRecipes.
//
// To use Pretty, import Pretty._ where needed and call DSL helpers
// from inside a call to Pretty.apply.
//
class Pretty[T] extends MidPriorityPrettyImplicits[T] {
  def apply(appender: RecipeAppender[T]): DocRecipe[T] = appender(Seq.empty)

  case object nothing extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = append
  }

  case class doc(doc: Doc) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddDoc(doc) +: append
  }

  case object break extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddBreak +: append
  }

  case class breakBefore(doc: RecipeAppender[T], break: RecipeAppender[T] = break) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = {
      val afterDoc = doc(Seq.empty)
      val brokenDoc = if (afterDoc.isEmpty) doc else break :: doc
      brokenDoc(append)
    }
  }

  val silentBreak: RecipeAppender[T] = breakWith("")

  case class breakWith(text: String) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddBreak(Some(text)) +: append
  }

  case object noBreak extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddNoBreak +: append
  }

  case class group(ops: RecipeAppender[T]) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushGroupFrame +: ops(PopFrame +: append)
  }

  case class nest(ops: RecipeAppender[T]) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushNestFrame +: ops(PopFrame +: append)
  }

  case class page(ops: RecipeAppender[T]) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = PushPageFrame +: ops(PopFrame +: append)
  }

  implicit class splicingAppender(recipe: DocRecipe[T]) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = recipe ++ append
  }

  case object splice {
    def apply(recipe: DocRecipe[T]) = new splicingAppender(recipe)
  }

  implicit class listAppender(recipes: TraversableOnce[RecipeAppender[T]]) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) =
      recipes.foldRight(append) { _.apply(_) }
  }

  case object list {
    def apply(recipes: TraversableOnce[RecipeAppender[T]]) = new listAppender(recipes)
  }

  case class breakList(recipes: TraversableOnce[RecipeAppender[T]], break: RecipeAppender[T] = break)
    extends RecipeAppender[T] {

    def apply(append: DocRecipe[T]) = recipes.foldRight(append) {
      case (hd, acc) if acc == append => hd(acc)
      case (hd, acc)                  => hd(break(acc))
    }
  }

  case class sepList(recipes: TraversableOnce[RecipeAppender[T]],
                     sep: RecipeAppender[T] = text(","),
                     break: RecipeAppender[T] = break)
    extends RecipeAppender[T] {

    def apply(append: DocRecipe[T]) = recipes.foldRight(append) {
      case (hd, acc) if acc == append => hd(acc)
      case (hd, acc)                  => hd(sep(break(acc)))
    }
  }

  case class groupedSepList(recipes: TraversableOnce[RecipeAppender[T]],
                            sep: RecipeAppender[T] = text(","),
                            break: RecipeAppender[T] = break)
    extends RecipeAppender[T] {

    def apply(append: DocRecipe[T]) = recipes.foldRight(append) {
      case (hd: RecipeAppender[T], acc: DocRecipe[T]) =>
        if (acc == append)
          hd(acc)
        else
          (group(hd :: sep) :: break)(acc)
    }
  }


  // TODO: Test these

  def block(name: RecipeAppender[T], open: RecipeAppender[T] = "(", close: RecipeAppender[T] = ")")(innerDoc: RecipeAppender[T]): RecipeAppender[T] =
    group( name :: surrounded(open, close, silentBreak, silentBreak )(innerDoc) )

  def brackets(innerDoc: RecipeAppender[T], break: RecipeAppender[T] = silentBreak) =
    surrounded(open = "[", close = "]", break, break)(innerDoc)

  def braces(innerDoc: RecipeAppender[T], break: RecipeAppender[T] = silentBreak) =
    surrounded(open = "{", close = "}", break, break)(innerDoc)

  def parens(innerDoc: RecipeAppender[T], break: RecipeAppender[T] = silentBreak) =
    surrounded(open = "(", close = ")", break, break)(innerDoc)

  def comment(innerDoc: RecipeAppender[T], break: RecipeAppender[T] = break) =
    surrounded(open = "/*", close = "*/", break, break)(innerDoc)

  case class surrounded(open: RecipeAppender[T],
                        close: RecipeAppender[T],
                        openBreak: RecipeAppender[T] = break,
                        closeBreak: RecipeAppender[T] = break)(innerDoc: RecipeAppender[T])
    extends RecipeAppender[T] {
      def apply(append: DocRecipe[T]) =
        group(open :: nest(group(breakBefore(innerDoc, openBreak))) :: breakBefore(close, closeBreak))(append)
  }
}

protected class MidPriorityPrettyImplicits[T] extends LowPriorityPrettyImplicits[T] {
  implicit class textAppender(text: String) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddText(text) +: append
  }

  case object text {
    def apply(value: String) = new textAppender(value)
  }

  // This allows writing Pretty(...) instead of Some(Pretty(...) when defining DocGens
  implicit def liftDocRecipe(opts: DocRecipe[T]): Some[DocRecipe[T]] = Some(opts)
}

protected class LowPriorityPrettyImplicits[T] {
  // Abstract "content" that still needs to be rendered into PrintableDocSteps
  //
  // The actual value is taken as a by name closure to be able to perform error
  // handling when dealing with buggy / partially unimplemented classes.
  //
  implicit class prettyAppender[+S <: T : TypeTag](content: => S) extends RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = AddPretty(content) +: append
  }

  case object pretty {
    def apply[S <: T : TypeTag](content: => S): prettyAppender[S] =
      new prettyAppender(content)
  }
}

object Pretty extends Pretty[Any] {
  case class literal(doc: Doc) extends RecipeAppender[Any] {
    def apply(append: DocRecipe[Any]) = AddPretty(DocLiteral(doc)) +: append
  }
}
