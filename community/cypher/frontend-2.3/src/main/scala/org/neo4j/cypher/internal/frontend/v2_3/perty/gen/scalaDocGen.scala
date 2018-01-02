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
package org.neo4j.cypher.internal.frontend.v2_3.perty.gen

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.format.{quoteChar, quoteString}
import org.neo4j.cypher.internal.frontend.v2_3.perty.helpers.{HasContainerType, HasMapType, HasType}
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.{Pretty, RecipeAppender}

import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._

case object scalaDocGen extends CustomDocGen[Any] {

  import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] =
    handleStringType(x) orElse
    handleCharType(x) orElse
    handleCharacterType(x) orElse
    handleArrayType(x) orElse
    handleMutableMapType(x) orElse
    handleImmutableMapType(x) orElse
    handleListType(x) orElse
    handleMutableSetType(x) orElse
    handleImmutableSetType(x) orElse
    handleSeqType(x) orElse
    handleProduct(x)

  case object handleStringType extends HasType[String, DocRecipe[Any]] {
    def mapTyped[X <: String : TypeTag](x: X) = Pretty(quoteString(x))
  }

  case object handleCharType extends HasType[Char, DocRecipe[Any]] {
    def mapTyped[X <: Char : TypeTag](x: X) = Pretty(quoteChar(x))
  }

  case object handleCharacterType extends HasType[java.lang.Character, DocRecipe[Any]] {
    def mapTyped[X <: java.lang.Character : TypeTag](x: X) = Pretty(quoteChar(x.charValue()))
  }

  case object handleArrayType extends HasContainerType[Array, DocRecipe[Any]] {
    def mapTyped[E : TypeTag](data: Array[E]) = {
      val elemTag = typeTag[E]
      val elems = data.map(pretty(_)(elemTag))
      Pretty(block("Array")(sepList(elems)))
    }

    def mapEmpty[E](data: Array[E]) = Pretty(text("Array()"))
  }

  case object handleImmutableMapType extends HasMapType[immutable.Map, DocRecipe[Any]] {
    def mapTyped[K : TypeTag, V : TypeTag](data: immutable.Map[K, V]) = {
      val keyTag = typeTag[K]
      val valTag = typeTag[V]
      val elems = data.map {
        case (k, v) => nest(group(pretty(k)(keyTag) :/: "→ " :: pretty(v)(valTag)))
      }
      Pretty(block("Map")(sepList(elems)))
    }

    def mapEmpty[K, V](data: immutable.Map[K, V]) = Pretty(text("Map()"))
  }

  case object handleMutableMapType extends HasMapType[mutable.Map, DocRecipe[Any]] {
    def mapTyped[K: TypeTag, V: TypeTag](data: mutable.Map[K, V]) = {
      val mapType = data.getClass.getSimpleName
      val keyTag = typeTag[K]
      val valTag = typeTag[V]
      val elems = data.map {
        case (k, v) => nest(group(pretty(k)(keyTag) :/: "→ " :: pretty(v)(valTag)))
      }
      Pretty(block(mapType)(sepList(elems)))
    }

    def mapEmpty[K, V](data: mutable.Map[K, V]) = Pretty(text(s"${data.getClass.getSimpleName}()"))
  }

  case object handleListType extends HasContainerType[List, DocRecipe[Any]] {
    def mapTyped[E : TypeTag](data: List[E]) = {
      val elemTag = typeTag[E]
      val elems = data.foldRight[RecipeAppender[Any]]("⬨") {
        case (v, acc) => pretty(v)(elemTag) :/: "⸬ " :: acc
      }
      Pretty(group(elems))
    }

    def mapEmpty[E](data: List[E]) = Pretty("⬨")
  }

  case object handleImmutableSetType extends HasContainerType[immutable.Set, DocRecipe[Any]] {
    def mapTyped[E : TypeTag](data: immutable.Set[E]) = {
      val elemTag = typeTag[E]
      val elems = data.map(pretty[E](_)(elemTag))
      Pretty(block("Set")(sepList(elems)))
    }

    def mapEmpty[E](data: immutable.Set[E]) = Pretty(text("Set()"))
  }

  case object handleMutableSetType extends HasContainerType[mutable.Set, DocRecipe[Any]] {
    def mapTyped[E : TypeTag](data: mutable.Set[E]) = {
      val setType = data.getClass.getSimpleName
      val elemTag = typeTag[E]
      val elems = data.map(pretty[E](_)(elemTag))
      Pretty(block(setType)(sepList(elems)))
    }

    def mapEmpty[E](data: mutable.Set[E]) = Pretty(text(s"${data.getClass.getSimpleName}()"))
  }

  case object handleSeqType extends HasContainerType[Seq, DocRecipe[Any]] {
    def mapTyped[E : TypeTag](data: Seq[E]) = {
      val seqType = data.getClass.getSimpleName
      val elemTag = typeTag[E]
      val elems = data.map(pretty[E](_)(elemTag))
      Pretty(block(seqType)(sepList(elems)))
    }

    def mapEmpty[E](data: Seq[E]) = Pretty(text(s"${data.getClass.getSimpleName}()"))
  }

  case object handleProduct extends HasType[Product, DocRecipe[Any]] {
    def mapTyped[X <: Product : TypeTag](product: X): Option[DocRecipe[Any]] = {
      val prefix = product.productPrefix
      if (product.productArity == 0) {
        val productName = if (prefix.startsWith("Tuple")) "()" else prefix
        Pretty(productName)
      } else {
        val productName = if (prefix.startsWith("Tuple")) "" else prefix
        val elems = product.productIterator.toSeq.map(pretty[Any])
        Pretty(block(productName)(sepList(elems)))
      }
    }
  }
}
