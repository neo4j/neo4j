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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.{Pretty, RecipeAppender}

import scala.reflect.runtime.universe.TypeTag

case object astPhraseDocGen extends CustomDocGen[ASTNode] {

  import Pretty._

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

  implicit class ClauseConverter(clause: Clause) extends Converter {
    def unquote = clause match {
      case clause: Return => clause.unquote
      case clause: With   => clause.unquote
      case clause: Unwind => clause.unquote
      case _              => text(clause.toString)
    }
  }
  abstract class ProjectionClauseConverter extends Converter {
    def clause: ProjectionClause
    def where: Option[Where]

    def unquote = {
      val distinct: RecipeAppender[Any] = if (clause.distinct) "DISTINCT" else nothing
      val items = pretty[ReturnItems](clause.returnItems)
      val orderBy = prettyOption(clause.orderBy)
      val skip = prettyOption(clause.skip)
      val limit = prettyOption(clause.limit)
      val predicate = prettyOption(where)
      section(clause.name)(distinct :/?: items :/?: predicate :/?: orderBy :/?: skip :/?: limit)
    }
  }

  implicit class ReturnConverter(val clause: Return) extends ProjectionClauseConverter {
    def where = None
  }

  implicit class WithConverter(val clause: With) extends ProjectionClauseConverter {
    def where = clause.where
  }

  implicit class ReturnItemsConverter(returnItems: ReturnItems) extends Converter {
    def unquote= if (returnItems.includeExisting && returnItems.items.isEmpty)
      "*"
    else if (returnItems.includeExisting)
      "*," :/: sepList(returnItems.items.map(pretty[ReturnItem]))
    else
      sepList(returnItems.items.map(pretty[ReturnItem]))
  }

  implicit class ReturnItemConverter(item: ReturnItem) extends Converter {
    def unquote = item match {
      case aliasedItem: AliasedReturnItem => aliasedItem.unquote
      case unAliasedItem: UnaliasedReturnItem => unAliasedItem.unquote
    }
  }

  implicit class AliasedReturnItemConverter(item: AliasedReturnItem) extends Converter{
    def unquote = group(pretty(item.expression) :/: "AS" :/: pretty(item.identifier))
  }

  implicit class UnaliasedReturnItemConverter(item: UnaliasedReturnItem) extends Converter {
    def unquote = item.inputText
  }

  implicit class WhereConverter(where: Where) extends Converter {
    def unquote = section("WHERE")(pretty(where.expression))
  }

  implicit class OrderByConverter(orderBy: OrderBy) extends Converter {
    def unquote =
      group("ORDER BY" :/: groupedSepList(orderBy.sortItems.map(pretty[SortItem])))
  }

  implicit class SortItemConverter(sortIem: SortItem) extends Converter {
    def unquote = sortIem match {
      case AscSortItem(expression) => pretty(expression)
      case DescSortItem(expression) =>group(pretty(expression) :/: "DESC")
    }
  }

  implicit class HintConverter(hint: Hint) extends PartialConverter {
    def unquote = hint match {
      case UsingIndexHint(identifier, label, property) =>
        Some(group("USING" :/: "INDEX" :/: group(pretty(identifier) :: block(pretty(label))(pretty(property)))))

      case UsingScanHint(identifier, label) =>
        Some(group("USING" :/: "SCAN" :/: group(pretty(identifier) :: pretty(label))))

      case UsingJoinHint(identifier) =>
        Some(group("USING" :/: "JOIN" :/: "ON" :/: pretty(identifier)))

      case _ =>
        None
    }
  }

  implicit class SlicingPhraseConverter(slice: ASTSlicingPhrase) extends Converter {
    def unquote = slice match {
      case Skip(expr) => section("SKIP")(pretty(expr))
      case Limit(expr) => section("LIMIT")(pretty(expr))
    }
  }

  implicit class UnwindConverter(unwind: Unwind) extends Converter {
    def unquote = {
      val input = pretty(unwind.expression)
      val output = pretty(unwind.identifier)
      section("UNWIND")(input :/: "AS" :/: output)
    }
  }
}
