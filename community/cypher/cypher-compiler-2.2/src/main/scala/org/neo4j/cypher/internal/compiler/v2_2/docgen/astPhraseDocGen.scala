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

case object astPhraseDocGen extends CustomDocGen[ASTNode] {
  def newDocDrill = mkDocDrill[ASTNode]() {
    case phrase: ASTPhrase =>
      inner =>
        astPhraseConverter(inner)(phrase)
  }
}

case class astPhraseConverter(pretty: DocConverter[Any]) extends (ASTPhrase => Option[Doc]) {
  def apply(phrase: ASTPhrase) = Try[Doc] {
    phrase match {
      case hint: Hint => hint.asDoc
      case orderBy: OrderBy => orderBy.asDoc
      case sortItem: SortItem => sortItem.asDoc
    }
  }.toOption

  implicit class expressionConverter(expression: Expression) {
    def asDoc = pretty(expression)
  }

  implicit class particleConverter(particle: ASTParticle) {
    def asDoc = pretty(particle)
  }

  implicit class OrderByConverter(orderBy: OrderBy) {
    def asDoc = group("ORDER BY" :/: groupedSepList(orderBy.sortItems.map(pretty)))
  }

  implicit class SortItemConverter(sortIem: SortItem) {
    def asDoc: Doc = sortIem match {
      case AscSortItem(expression) => expression.asDoc
      case DescSortItem(expression) => group(expression.asDoc :/: "DESC")
    }
  }

  implicit class HintConverter(hint: Hint) {
    def asDoc = hint match {
      case UsingIndexHint(identifier, label, property) =>
        group("USING" :/: "INDEX" :/: group(pretty(identifier) :: block(pretty(label))(pretty(property))))

      case UsingScanHint(identifier, label) =>
        group("USING" :/: "SCAN" :/: group(pretty(identifier) :: pretty(label)))
    }
  }
}
