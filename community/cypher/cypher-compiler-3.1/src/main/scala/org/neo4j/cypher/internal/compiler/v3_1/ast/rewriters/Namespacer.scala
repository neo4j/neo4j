/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.Namespacer.VariableRenamings
import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.{Ref, Rewriter, SemanticTable, bottomUp, _}

object Namespacer {

  type VariableRenamings = Map[Ref[Variable], Variable]

  def apply(statement: Statement, scopeTree: Scope): Namespacer = {
    val ambiguousNames = shadowedNames(scopeTree)
    val variableDefinitions: Map[SymbolUse, SymbolUse] = scopeTree.allVariableDefinitions
    val protectedVariables = returnAliases(statement)
    val renamings = variableRenamings(statement, variableDefinitions, ambiguousNames, protectedVariables)
    Namespacer(renamings)
  }

  private def shadowedNames(scopeTree: Scope): Set[String] = {
    val definitions = scopeTree.allSymbolDefinitions

    definitions.collect {
      case (name, symbolDefinitions) if symbolDefinitions.size > 1 => name
    }.toSet
  }

  private def returnAliases(statement: Statement): Set[Ref[Variable]] =
    statement.treeFold(Set.empty[Ref[Variable]]) {

      // ignore variable in StartItem that represents index names and key names
      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        val variables = items.map(_.alias.map(Ref[Variable]).get)
        acc => (acc ++ variables, Some(identity))
    }

  private def variableRenamings(statement: Statement, variableDefinitions: Map[SymbolUse, SymbolUse],
                                  ambiguousNames: Set[String], protectedVariables: Set[Ref[Variable]]): VariableRenamings =
    statement.treeFold(Map.empty[Ref[Variable], Variable]) {
      case i: Variable if ambiguousNames(i.name) && !protectedVariables(Ref(i)) =>
        val symbolDefinition = variableDefinitions(i.toSymbolUse)
        val newVariable = i.renameId(s"  ${symbolDefinition.nameWithPosition}")
        val renaming = Ref(i) -> newVariable
        acc => (acc + renaming, Some(identity))
    }
}

case class Namespacer(renamings: VariableRenamings) {
  val statementRewriter: Rewriter = inSequence(
    bottomUp(Rewriter.lift {
      case item@ProcedureResultItem(None, v: Variable) if renamings.contains(Ref(v)) =>
        item.copy(output = Some(ProcedureOutput(v.name)(v.position)))(item.position)
    }),
    bottomUp(Rewriter.lift {
      case v: Variable =>
        renamings.get(Ref(v)) match {
          case Some(newVariable) => newVariable
          case None              => v
        }
    }))

  val tableRewriter = (semanticTable: SemanticTable) => {
    val replacements = renamings.toIndexedSeq.collect { case (old, newVariable) => old.value -> newVariable }
    val newSemanticTable = semanticTable.replaceVariables(replacements: _*)
    newSemanticTable
  }
}
