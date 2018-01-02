/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.{Ref, Rewriter, bottomUp, inSequence}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseContext, BaseState, Condition, Phase}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{Scope, SemanticTable, SymbolUse}
import org.neo4j.cypher.internal.v3_4.expressions.{LogicalVariable, ProcedureOutput, Variable}

object Namespacer extends Phase[BaseContext, BaseState, BaseState] {
  type VariableRenamings = Map[Ref[Variable], Variable]

  import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE

  override def phase: CompilationPhase = AST_REWRITE

  override def description: String = "rename variables so they are all unique"

  override def process(from: BaseState, ignored: BaseContext): BaseState = {
    val ambiguousNames = shadowedNames(from.semantics().scopeTree)
    val variableDefinitions: Map[SymbolUse, SymbolUse] = from.semantics().scopeTree.allVariableDefinitions
    val protectedVariables = returnAliases(from.statement())
    val renamings = variableRenamings(from.statement(), variableDefinitions, ambiguousNames, protectedVariables)

    val rewriter = renamingRewriter(renamings)
    val newStatement = from.statement().endoRewrite(rewriter)
    val table = SemanticTable(types = from.semantics().typeTable, recordedScopes = from.semantics().recordedScopes)

    val newSemanticTable = table.replaceExpressions(rewriter)
    from.withStatement(newStatement).withSemanticTable(newSemanticTable)
  }

  override def postConditions: Set[Condition] = Set.empty

  private def shadowedNames(scopeTree: Scope): Set[String] = {
    val definitions = scopeTree.allSymbolDefinitions

    definitions.collect {
      case (name, symbolDefinitions) if symbolDefinitions.size > 1 => name
    }.toSet
  }

  private def returnAliases(statement: Statement): Set[Ref[LogicalVariable]] =
    statement.treeFold(Set.empty[Ref[LogicalVariable]]) {

      case With(_, _, GraphReturnItems(_, items), _, _, _, _) =>
        val gVars = extractGraphVars(items)
        acc => (acc ++ gVars, Some(identity))

      // ignore variable in StartItem that represents index names and key names
      case Return(_, ReturnItems(_, items), graphItems, _, _, _, _) =>
        val variables = items.map(_.alias.map(Ref[LogicalVariable]).get)
        val gVars = graphItems.map(_.items).map(extractGraphVars).getOrElse(Seq.empty)
        acc => (acc ++ variables ++ gVars, Some(identity))
    }

  private def extractGraphVars(items: Seq[GraphReturnItem]): Seq[Ref[LogicalVariable]] = {
    items.flatMap { item =>
      item.graphs.flatMap {
        case g: GraphAs =>
          Seq(g.ref, g.as.get).map(Ref[Variable])
        case x =>
          x.as.map(Ref[Variable])
      }
    }
  }

  private def variableRenamings(statement: Statement, variableDefinitions: Map[SymbolUse, SymbolUse],
                                ambiguousNames: Set[String], protectedVariables: Set[Ref[LogicalVariable]]): VariableRenamings =
    statement.treeFold(Map.empty[Ref[Variable], Variable]) {
      case i: Variable if ambiguousNames(i.name) && !protectedVariables(Ref(i)) =>
        val symbolDefinition = variableDefinitions(SymbolUse(i))
        val newVariable = i.renameId(s"  ${symbolDefinition.nameWithPosition}")
        val renaming = Ref(i) -> newVariable
        acc => (acc + renaming, Some(identity))
    }

  private def renamingRewriter(renamings: VariableRenamings): Rewriter = inSequence(
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

}
