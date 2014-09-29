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
import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
import org.neo4j.cypher.internal.compiler.v2_2.perty._

import scala.util.Try

case object astPhraseDocGen { // extends CustomDocGen[ASTNode] {

  def drill = {
    ???
//    val phraseDocDrill = mkDocDrill[ASTPhrase]() {
//      case clause: Clause => clause.asDoc
//      case item: ReturnItem => item.asDoc
//      case items: ReturnItems => items.asDoc
//      case where: Where => where.asDoc
//      case hint: Hint => hint.asDoc
//      case orderBy: OrderBy => orderBy.asDoc
//      case sortItem: SortItem => sortItem.asDoc
//      case slice: ASTSlicingPhrase => slice.asDoc
//    }
//
//    {
//      case phrase: ASTPhrase => inner => phraseDocDrill(phrase)(inner)
//      case _                 => inner => None
//    }
  }
//
//  implicit class ClauseConverter(clause: Clause) {
//    def asDoc(pretty: DocConverter[Any]): Doc = clause match {
//      case clause: Return => clause.asDoc(pretty)
//      case clause: With   => clause.asDoc(pretty)
//      case clause: Unwind => clause.asDoc(pretty)
//      case _              => TextDoc(clause.toString)
//    }
//  }
//  abstract class ProjectionClauseConverter {
//    def clause: ProjectionClause
//    def where: Option[Where]
//
//    def asDoc(pretty: DocConverter[Any]): Doc = {
//      val distinct: Doc = if (clause.distinct) "DISTINCT" else nil
//      val items: Doc = pretty(clause.returnItems)
//      val orderBy: Doc = clause.orderBy.map(pretty)
//      val skip: Doc = clause.skip.map(pretty)
//      val limit: Doc = clause.limit.map(pretty)
//      val predicate: Doc = where.map(pretty)
//      section(clause.name, distinct :+: items :+: predicate :+: orderBy :+: skip :+: limit)
//    }
//  }
//
//  implicit class ReturnConverter(val clause: Return) extends ProjectionClauseConverter {
//    def where = None
//  }
//
//  implicit class WithConverter(val clause: With) extends ProjectionClauseConverter {
//    def where = clause.where
//  }
//
//  implicit class ReturnItemsConverter(returnItems: ReturnItems) {
//    def asDoc(pretty: DocConverter[Any]): Doc = if (returnItems.includeExisting && returnItems.items.isEmpty)
//      text("*")
//    else if (returnItems.includeExisting)
//      text("*,") :/: sepList(returnItems.items.map(pretty))
//    else
//      sepList(returnItems.items.map(pretty))
//  }
//
//  implicit class ReturnItemConverter(item: ReturnItem) {
//    def asDoc(pretty: DocConverter[Any]): Doc = item match {
//      case aliasedItem: AliasedReturnItem => aliasedItem.asDoc(pretty)
//      case unAliasedItem: UnaliasedReturnItem => unAliasedItem.asDoc(pretty)
//    }
//  }
//
//  implicit class AliasedReturnItemConverter(item: AliasedReturnItem) {
//    def asDoc(pretty: DocConverter[Any]) = group(pretty(item.expression) :/: "AS" :/: pretty(item.identifier))
//  }
//
//  implicit class UnaliasedReturnItemConverter(item: UnaliasedReturnItem) {
//    def asDoc(pretty: DocConverter[Any]) = text(item.inputText)
//  }
//
//  implicit class WhereConverter(where: Where) {
//    def asDoc(pretty: DocConverter[Any]) = section("WHERE", pretty(where.expression))
//  }
//
//  implicit class OrderByConverter(orderBy: OrderBy) {
//    def asDoc(pretty: DocConverter[Any]) =
//      group("ORDER BY" :/: groupedSepList(orderBy.sortItems.map(pretty)))
//  }
//
//  implicit class SortItemConverter(sortIem: SortItem) {
//    def asDoc(pretty: DocConverter[Any]): Doc = sortIem match {
//      case AscSortItem(expression) => pretty(expression)
//      case DescSortItem(expression) => group(pretty(expression) :/: "DESC")
//    }
//  }
//
//  implicit class HintConverter(hint: Hint) {
//    def asDoc(pretty: DocConverter[Any]) = hint match {
//      case UsingIndexHint(identifier, label, property) =>
//        group("USING" :/: "INDEX" :/: group(pretty(identifier) :: block(pretty(label))(pretty(property))))
//
//      case UsingScanHint(identifier, label) =>
//        group("USING" :/: "SCAN" :/: group(pretty(identifier) :: pretty(label)))
//    }
//  }
//
//  implicit class SlicingPhraseConverter(slice: ASTSlicingPhrase) {
//    def asDoc(pretty: DocConverter[Any]) = slice match {
//      case Skip(expr) => section("SKIP", pretty(expr))
//      case Limit(expr) => section("LIMIT", pretty(expr))
//    }
//  }
//
//  implicit class UnwindConverter(unwind: Unwind) {
//    def asDoc(pretty: DocConverter[Any]) = {
//      val input: Doc = pretty(unwind.expression)
//      val output: Doc = pretty(unwind.identifier)
//      section("UNWIND", input :/: "AS" :/: output)
//    }
//  }
}
