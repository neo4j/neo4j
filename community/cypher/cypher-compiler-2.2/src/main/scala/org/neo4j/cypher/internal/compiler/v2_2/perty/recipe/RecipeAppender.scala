package org.neo4j.cypher.internal.compiler.v2_2.perty.recipe

import org.neo4j.cypher.internal.compiler.v2_2.perty.DocRecipe
import org.neo4j.cypher.internal.compiler.v2_2.perty.step.AddBreak

trait RecipeAppender[T] extends (DocRecipe[T] => DocRecipe[T]) {
  self =>

  def ::(hd: RecipeAppender[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = hd(self(append))
  }

  def :/:(hd: RecipeAppender[T]) = new RecipeAppender[T] {
    def apply(append: DocRecipe[T]) = hd(AddBreak +: self(append))
  }
}
