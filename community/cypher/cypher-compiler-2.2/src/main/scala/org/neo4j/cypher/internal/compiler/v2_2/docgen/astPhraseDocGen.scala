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

package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.{Pretty, RecipeAppender}

import scala.reflect.runtime.universe.TypeTag

case object astPhraseDocGen extends CustomDocGen[ASTNode] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
    case clause: Clause => clause.asPretty
    case item: ReturnItem => item.asPretty
    case items: ReturnItems => items.asPretty
    case where: Where => where.asPretty
    case hint: Hint => hint.asPretty
    case orderBy: OrderBy => orderBy.asPretty
    case sortItem: SortItem => sortItem.asPretty
    case slice: ASTSlicingPhrase => slice.asPretty
    case _ => None
  }

  implicit class ClauseConverter(clause: Clause) {
    def asPretty: DocRecipe[Any] = clause match {
      case clause: Return => clause.asPretty
      case clause: With   => clause.asPretty
      case clause: Unwind => clause.asPretty
      case _              => Pretty(text(clause.toString))
    }
  }
  abstract class ProjectionClauseConverter {
    def clause: ProjectionClause
    def where: Option[Where]

    def asPretty: DocRecipe[Any] = {
      val distinct: RecipeAppender[Any] = if (clause.distinct) Pretty("DISTINCT") else nothing
      val items = pretty[ReturnItems](clause.returnItems)
      val orderBy = prettyOption(clause.orderBy)
      val skip = prettyOption(clause.skip)
      val limit = prettyOption(clause.limit)
      val predicate = prettyOption(where)
      Pretty(section(clause.name)(distinct :/?: items :/?: predicate :/?: orderBy :/?: skip :/?: limit))
    }
  }

  implicit class ReturnConverter(val clause: Return) extends ProjectionClauseConverter {
    def where = None
  }

  implicit class WithConverter(val clause: With) extends ProjectionClauseConverter {
    def where = clause.where
  }

  implicit class ReturnItemsConverter(returnItems: ReturnItems) {
    def asPretty: DocRecipe[Any] = if (returnItems.includeExisting && returnItems.items.isEmpty)
      Pretty("*")
    else if (returnItems.includeExisting)
      Pretty("*," :/: sepList(returnItems.items.map(pretty[ReturnItem])))
    else
      Pretty(sepList(returnItems.items.map(pretty[ReturnItem])))
  }

  implicit class ReturnItemConverter(item: ReturnItem) {
    def asPretty: DocRecipe[Any] = item match {
      case aliasedItem: AliasedReturnItem => aliasedItem.asPretty
      case unAliasedItem: UnaliasedReturnItem => unAliasedItem.asPretty
    }
  }

  implicit class AliasedReturnItemConverter(item: AliasedReturnItem) {
    def asPretty = Pretty(group(pretty(item.expression) :/: "AS" :/: pretty(item.identifier)))
  }

  implicit class UnaliasedReturnItemConverter(item: UnaliasedReturnItem) {
    def asPretty = Pretty(item.inputText)
  }

  implicit class WhereConverter(where: Where) {
    def asPretty = Pretty(section("WHERE")(pretty(where.expression)))
  }

  implicit class OrderByConverter(orderBy: OrderBy) {
    def asPretty =
      Pretty(group("ORDER BY" :/: groupedSepList(orderBy.sortItems.map(pretty[SortItem]))))
  }

  implicit class SortItemConverter(sortIem: SortItem) {
    def asPretty: DocRecipe[Any] = sortIem match {
      case AscSortItem(expression) => Pretty(pretty(expression))
      case DescSortItem(expression) => Pretty(group(pretty(expression) :/: "DESC"))
    }
  }

  implicit class HintConverter(hint: Hint) {
    def asPretty: Option[DocRecipe[Any]] = hint match {
      case UsingIndexHint(identifier, label, property) =>
        Pretty(group("USING" :/: "INDEX" :/: group(pretty(identifier) :: block(pretty(label))(pretty(property)))))

      case UsingScanHint(identifier, label) =>
        Pretty(group("USING" :/: "SCAN" :/: group(pretty(identifier) :: pretty(label))))

      case _ =>
        None
    }
  }

  implicit class SlicingPhraseConverter(slice: ASTSlicingPhrase) {
    def asPretty = slice match {
      case Skip(expr) => Pretty(section("SKIP")(pretty(expr)))
      case Limit(expr) => Pretty(section("LIMIT")(pretty(expr)))
    }
  }

  implicit class UnwindConverter(unwind: Unwind) {
    def asPretty = {
      val input = pretty(unwind.expression)
      val output = pretty(unwind.identifier)
      Pretty(section("UNWIND")(input :/: "AS" :/: output))
    }
  }
}
