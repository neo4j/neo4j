/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.namespaceIdentifiers.IdentifierNames
import org.neo4j.cypher.internal.compiler.v2_2.ast.{AliasedReturnItem, ReturnItems, Return, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable

import scala.collection.mutable

object namespaceIdentifiers {
  type IdentifierNames = Map[(String, InputPosition), String]
}

case class namespaceIdentifiers(scopeTree: Scope) extends Rewriter {
  val identifierNames = namespacedIdentifierNames(scopeTree)

  def apply(in: AnyRef): AnyRef = bottomUp(getRewriter(in)).apply(in)

  import Foldable._
  private def findReturnIdentifiers(any: AnyRef): Seq[Identifier] = {
    any.treeFold(Seq.empty[Identifier]) {
      case Return(_, ReturnItems(_, items), _, _, _) =>
        (acc, children) => children(acc ++ items.map(_.alias.get))
    }
  }

  private def getRewriter(any: AnyRef): Rewriter = {
    val blacklisted = findReturnIdentifiers(any)
    Rewriter.lift {
      case i: Identifier if !blacklisted.exists(_ eq i) =>
        identifierNames.get((i.name, i.position)).fold(i)(Identifier(_)(i.position))
    }
  }

  private def findAllSymbols(scope: Scope): Seq[Symbol] =
    scope.symbolTable.foldLeft(Seq.empty[Symbol]) {
      case (acc, (name, symbol)) => acc :+ symbol
    } ++ scope.children.flatMap(findAllSymbols)

  /*
   Find clusters of symbols that can be proven to point to the same value.
   */
  private def findClusters(symbols: Seq[Symbol]) = {
    val map = symbols.flatMap(symbol => symbol.positions.map(_ -> symbol)).toMap
    val initialStack = map.keys.zip(map.keys).toList
    val (_, clustering) = Stream.iterate(initialStack -> Map.empty[InputPosition, InputPosition]) {
      case (Nil, clusters) =>
        Nil -> clusters
      case (stack, clusters) =>
        val (cluster, position) :: _ = stack
        if (!clusters.contains(position)) {
          val newStack = map(position).positions.map(cluster -> _).toList ++ stack.tail
          val newClusters = clusters + (position -> cluster)
          newStack -> newClusters
        } else {
          stack.tail -> clusters
        }
    }.find(_._1.isEmpty).get
    symbols.groupBy(symbol => clustering(symbol.positions.head))
  }

  private def findShadowedIdentifiers(scope: Scope): Seq[Symbol] = {
    findClusters(findAllSymbols(scope))
      .values
      .toSeq
      .groupBy(_.head.name)
      .filter { case (_, symbols) => symbols.length > 1}
      .values
      .flatten
      .flatten
      .toSeq
  }

  private def namespacedIdentifierNames(scope: Scope): IdentifierNames =
    findShadowedIdentifiers(scope).flatMap { symbol =>
      val firstPosition = symbol.positions.head
      symbol.positions.map { position =>
        (symbol.name, position) -> s"  ${symbol.name}@${firstPosition.offset}"
      }
    }.toMap
}
