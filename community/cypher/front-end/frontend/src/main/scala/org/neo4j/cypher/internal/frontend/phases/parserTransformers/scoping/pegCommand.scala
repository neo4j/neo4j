/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.CommandClauseWithNames
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.WaitableAdministrationCommand
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAdministrationCommand
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

object pegCommand {

  /*
   * Note that scope caching for commands via PegContext.getRecordScopeOrElse is already covered by pegStatement.
   */
  def apply(command: AdministrationCommand, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = command
    command match {

      /**
       * ReadAdministrationCommand
       */
      case read: ReadAdministrationCommand =>
        val defaultCols = read.defaultColumnVariables
        val declaringAST = read.withYieldOrWhere(None)
        val declaringScope = StatementScope(
          declaringAST,
          incoming,
          References.empty,
          Declarations(Seq.empty, defaultCols),
          incoming.amendedWith(defaultCols.toSet),
          TableResult(defaultCols)
        )

        val graphSelectionScope = read.useGraph.map(gs =>
          pegExpression(gs.graphReference, declaringScope.outgoing)
        )

        val yieldOrWhereScopes = read.yieldOrWhere match {
          case Some(Left((yieldClause, None))) =>
            Seq(pegClause(yieldClause, declaringScope.outgoing))
          case Some(Left((yieldClause, optReturn))) =>
            val yieldScope = pegClause(yieldClause, declaringScope.outgoing)
            val returnScope = optReturn.map(pegClause(_, yieldScope.outgoing))
            Seq(yieldScope) ++ returnScope
          case Some(Right(whereClause)) =>
            Seq(pegExpression(
              whereClause.expression,
              declaringScope.outgoing.constantChildContext()
            ))
          case None => Seq.empty
        }

        val hasYield = read.yields.isDefined
        val children = Seq(Some(declaringScope), graphSelectionScope, yieldOrWhereScopes).flatten
        val outgoing = if (hasYield) children.last.outgoing else children.head.outgoing
        val result = if (hasYield) children.last.result else children.head.result

        incoming.resultScope(outgoing, result, children, None)

      /**
       * WriteAdministrationCommand
       */
      case wait: WaitableAdministrationCommand if c.language == CypherVersion.Cypher5 =>
        val defaultCols = wait.returnColumns
        val outgoing = incoming.amendedWith(defaultCols.toSet)
        val graphSelectionScope = wait.useGraph.map(gs =>
          pegExpression(gs.graphReference, outgoing)
        )
        incoming.resultScope(
          outgoing,
          TableResult(defaultCols),
          graphSelectionScope.toSeq,
          None,
          Declarations(Seq.empty, defaultCols)
        )
      case write: WriteAdministrationCommand =>
        val graphSelectionScope = write.useGraph.map(gs =>
          pegExpression(gs.graphReference, incoming)
        )
        incoming.omittedResultScope(RegularContext.unit, graphSelectionScope.toSeq)

    }
  }

  def apply(command: SchemaCommand, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = command
    command match {

      /**
       * Schema Commands
       */
      case ci: CreateLookupIndex =>
        val graphSelectionScope = ci.useGraph.map(gs =>
          pegExpression(gs.graphReference, incoming)
        )
        val partsIncoming = incoming.amendedWithConstant(ci.variable)
        val functionScope = pegExpression(ci.function, partsIncoming)
        val propertiesScopes = ci.properties.map(p => pegExpression(p, partsIncoming))
        val optionsScope = scopeOptions(ci.options, incoming)
        val children = graphSelectionScope.toSeq ++ Seq(functionScope) ++ propertiesScopes ++ optionsScope.toSeq
        val references = WorkingScope.referencedInChildren(children).filterTargets(t => t != ci.variable)
        incoming.omittedResultScope(RegularContext.unit, children, Some(references))
      case ci: CreateIndex =>
        val graphSelectionScope = ci.useGraph.map(gs =>
          pegExpression(gs.graphReference, incoming)
        )
        val propertiesScopes = ci.properties.map(p => pegExpression(p, incoming.amendedWithConstant(ci.variable)))
        val optionsScope = scopeOptions(ci.options, incoming)
        val children = (graphSelectionScope.toSeq ++ propertiesScopes) ++ optionsScope.toSeq
        val references = WorkingScope.referencedInChildren(children).filterTargets(t => t != ci.variable)
        incoming.omittedResultScope(RegularContext.unit, children, Some(references))

      case cc: CreateConstraint =>
        val graphSelectionScope = cc.useGraph.map(gs =>
          pegExpression(gs.graphReference, incoming)
        )
        val partsIncoming = incoming.amendedWithConstant(cc.variable)
        val propertiesScopes = cc.properties.map(p => pegExpression(p, partsIncoming))
        val optionsScopes = scopeOptions(cc.options, incoming)
        val children = (graphSelectionScope.toSeq ++ propertiesScopes) ++ optionsScopes.toSeq
        val references = WorkingScope.referencedInChildren(children).filterTargets(t => t != cc.variable)
        incoming.omittedResultScope(RegularContext.unit, children, Some(references))

      case agt: AlterCurrentGraphType =>
        val graphSelectionScope =
          agt.useGraph.map(gs => pegExpression(gs.graphReference, incoming))
        val graphTypeScope = None

        incoming.omittedResultScope(RegularContext.unit, graphTypeScope.toSeq ++ graphSelectionScope)
      case schemaCommand: SchemaCommand =>
        val graphSelectionScope = schemaCommand.useGraph.map(gs =>
          pegExpression(gs.graphReference, incoming)
        )
        incoming.omittedResultScope(RegularContext.unit, graphSelectionScope.toSeq)
    }
  }

  def apply(command: CommandClause, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    command match {

      /**
       * Show and terminate command clauses
       */
      case cmd: CommandClauseWithNames =>
        scopeCommandClause(cmd, incoming, Some(cmd.names))
      case cmd: CommandClause =>
        scopeCommandClause(cmd, incoming, None)
    }
  }

  private def scopeCommandClause(
    command: CommandClause,
    incoming: RegularContext,
    namesOpt: Option[CommandClauseNames]
  )(implicit c: PegContext): WorkingScope = {
    val (yieldWithOpt, yieldItems, yieldAll, whereOpt, position) =
      (command.yieldWith, command.yieldItems, command.yieldAll, command.where, command.position)

    val commandCols = command.getFilteredColumns(c.semanticFeatures)

    val accessedCols =
      if (yieldAll || yieldItems.isEmpty) commandCols
      else commandCols.filter(cc => yieldItems.exists(_.originalName == cc.name))

    val commandScope = StatementScope(
      command.getClauseWithoutSubclauses,
      RegularContext.unit,
      References.empty,
      Declarations.noDeclarations,
      outgoing = incoming.amendedWith(accessedCols.toSet),
      result = TableResult(accessedCols)
    )

    val declared =
      if (yieldAll || yieldItems.isEmpty) commandCols.map(v => AliasedReturnItem(v, v)(v.position))
      else yieldItems.flatMap(yi => Seq(yi.toReturnItem)).distinct

    val declaredWithIncoming =
      declared
        .filter(x => !incoming.variables.exists(_.name == x.name))
        .map(r => r.alias.getOrElse(Variable(r.name)(r.position, isIsolated = false)))

    val declaredByName: Map[String, LogicalVariable] =
      declaredWithIncoming.iterator.map(v => v.name -> v).toMap
    val commandOutgoingForYield = commandScope.outgoing.replaceWith(
      commandScope.outgoing.variables.map(v => declaredByName.getOrElse(v.name, v))
    )

    val incomingWithDefaults = commandOutgoingForYield
      .amendedWithConstant(incoming.constants)
      .amendedWith(incoming.variables)

    val namesScope = namesOpt.flatMap(n =>
      n.maybeExpression.map(expr => pegExpression(expr, incomingWithDefaults.constantChildContext()))
    )

    val modifiedYield = yieldWithOpt match {
      case Some(yW @ With(_, returnItems, _, _, _, _, _, _)) =>
        yW.copy(returnItems =
          returnItems.copy(projectionType = FreeProjection, items = declared)(returnItems.position)
        )(yW.position)
      case _ =>
        Yield(ReturnItems(FreeProjection, declared)(position), None, None, None, whereOpt)(position)
    }

    val yieldScope = pegClause(modifiedYield, incomingWithDefaults)

    val outgoing = yieldScope.outgoing
      .amendedWithConstant(incoming.constants)
      .amendedWith(incoming.variables)

    val whereScope = whereOpt.map(w => pegExpression(w.expression, outgoing.constantChildContext()))

    val children = Seq(commandScope) ++ namesScope ++ Seq(yieldScope) ++ whereScope
    val result = TableResult(outgoing.variables.toSeq)
    val declarations = Declarations(Seq.empty, declaredWithIncoming)

    incoming.resultScope(outgoing, result, children, None, declarations)(command)
  }

  private def scopeOptions(options: Options, incoming: RegularContext)(implicit c: PegContext): Option[WorkingScope] = {
    options match {
      case OptionsMap(map) =>
        val keys = map.map {
          case (key, expression) => (PropertyKeyName(key)(expression.position), expression)
        }.toSeq
        val mapExpression = MapExpression(keys)(options.position)
        Some(pegExpression(mapExpression, incoming))
      case OptionsParam(_) => None
      case NoOptions       => None
    }
  }
}
