/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.Namespacer.IdentifierRenamings
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable

object Namespacer {

  import org.neo4j.cypher.internal.compiler.v2_2.Foldable._

  type IdentifierRenamings = Map[Ref[Identifier], Identifier]

  def apply(statement: Statement, scopeTree: Scope): Namespacer = {
    val ambiguousNames = shadowedNames(scopeTree)
    val identifierDefinitions: Map[SymbolUse, SymbolUse] = scopeTree.allIdentifierDefinitions
    val protectedIdentifiers = returnAliases(statement)
    val renamings = identifierRenamings(statement, identifierDefinitions, ambiguousNames, protectedIdentifiers)
    Namespacer(renamings)
  }

  private def shadowedNames(scopeTree: Scope): Set[String] = {
    val definitions = scopeTree.allSymbolDefinitions

    definitions.collect {
      case (name, symbolDefinitions) if symbolDefinitions.size > 1 => name
    }.toSet
  }

  private def returnAliases(statement: Statement): Set[Ref[Identifier]] =
    statement.treeFold(Set.empty[Ref[Identifier]]) {

      // ignore identifier in StartItem that represents index names and key names
      case NodeByIdentifiedIndex(_, index, key, _) =>
        (acc, children) => children(acc ++ Seq(Ref(index), Ref(key)))
      case RelationshipByIdentifiedIndex(_, index, key, _) =>
        (acc, children) => children(acc ++ Seq(Ref(index), Ref(key)))
      case NodeByIndexQuery(_, index, _) =>
        (acc, children) => children(acc ++ Seq(Ref(index)))
      case RelationshipByIndexQuery(_, index, _) =>
        (acc, children) => children(acc ++ Seq(Ref(index)))
      case UsingIndexHint(_, _, prop) =>
        (acc, children) => children(acc ++ Seq(Ref(prop)))
      case Return(_, ReturnItems(_, items), _, _, _) =>
        val identifiers = items.map(_.alias.map(Ref[Identifier]).get)
        (acc, children) => children(acc ++ identifiers)
    }

  private def identifierRenamings(statement: Statement, identifierDefinitions: Map[SymbolUse, SymbolUse],
                                  ambiguousNames: Set[String], protectedIdentifiers: Set[Ref[Identifier]]): IdentifierRenamings =
    statement.treeFold(Map.empty[Ref[Identifier], Identifier]) {
      case i: Identifier if ambiguousNames(i.name) && !protectedIdentifiers(Ref(i)) =>
        val symbolDefinition = identifierDefinitions(i.toSymbolUse)
        val newIdentifier = i.renameId(s"  ${symbolDefinition.nameWithPosition}")
        val renaming = Ref(i) -> newIdentifier
        (acc, children) => children(acc + renaming)
    }
}

case class Namespacer(renamings: IdentifierRenamings) {
  val statementRewriter: Rewriter = bottomUp(Rewriter.lift {
    case i: Identifier =>
      renamings.get(Ref(i)) match {
        case Some(newIdentifier) => newIdentifier
        case None                => i
      }
  })

  val tableRewriter = (semanticTable: SemanticTable) => {
    val replacements = renamings.toSeq.collect { case (old, newIdentifier) => old.value -> newIdentifier }
    val newSemanticTable = semanticTable.replaceKeys(replacements: _*)
    newSemanticTable
  }
}
