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

import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.ProjectingUnion
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UnmappedUnion
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalFunctionScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalProcedureScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.UnexpectedAstNodeScopingError
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.unitVariables
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

object pegStatement {

  def apply(
    statement: Statement,
    incoming: RegularContext,
    inImportingWith: Boolean = false,
    foreachIterVar: Option[LogicalVariable] = None
  )(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Statement](
      statement,
      incoming,
      inImportingWith,
      foreachIterVar,
      applyUncached(_, _, inImportingWith, foreachIterVar)
    )
  }

  private def applyUncached(
    statement: Statement,
    incoming: RegularContext,
    inImportingWith: Boolean,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = statement
    statement match {

      /**
       * Statement
       */
      case QueryWithLocalDefinitions(definitions, query) =>
        val definitionsChildren = definitions.scanLeft(WorkingScope.apriori(incoming.keepOnlyLocalCallable())) {
          case (previous, lcd) =>
            val bodyIncoming = previous.outgoing.amendedWithConstant(lcd.inputSignature.map(lfs =>
              Variable(lfs.name)(lfs.position, Variable.isIsolatedDefault).asInstanceOf[LogicalVariable]
            ).toSet)
            val (bodyChild, localCallableScopeSignature): (WorkingScope, LocalCallableScopeSignature) = lcd match {
              case lpd @ LocalProcedureDefinition(name, inputSignature, outputSignatureOpt, body) =>
                val bodyChild = apply(body, bodyIncoming)
                val definitionResult = outputSignatureOpt.map(outputSignature =>
                  TableResult(outputSignature.map(lfs =>
                    Variable(lfs.name)(lfs.position, Variable.isIsolatedDefault).asInstanceOf[LogicalVariable]
                  ))
                ).getOrElse(bodyChild.result)
                (
                  bodyChild,
                  LocalProcedureScopeSignature(
                    name,
                    inputSignature,
                    lpd.inferredOutputSignatureWithoutTypes,
                    definitionResult
                  )
                )
              case LocalFunctionDefinition(name, inputSignature, outputSignature, body) =>
                val bodyChild = body match {
                  case QueryBody(query)           => apply(query, bodyIncoming)
                  case ExpressionBody(expression) => pegExpression(expression, bodyIncoming)
                }
                val definitionResult = ExpressionResult
                (bodyChild, LocalFunctionScopeSignature(name, inputSignature, outputSignature, definitionResult))
            }
            val referenced = Some(References.empty)
            val declared = Declarations.ofLocalCallable(localCallableScopeSignature)
            val outgoing = previous.outgoing.amendedWithLocalCallable(localCallableScopeSignature)
            implicit val astNode: ASTNode = lcd
            previous.outgoing.noResultScope(outgoing, Seq(bodyChild), referenced, declared)
        }.tail
        val queryIncoming = definitionsChildren.lastOption.map(definitionChild =>
          incoming.amendedWithLocalCallables(definitionChild.outgoing.localCallables)
        ).getOrElse(incoming)
        val queryChild = apply(query, queryIncoming, foreachIterVar = foreachIterVar)
        val outgoing = queryChild.outgoing.amendedWithLocalCallables(incoming.localCallables)
        val children = definitionsChildren :+ queryChild
        incoming.resultScope(outgoing, queryChild.result, children)
      case NextStatement(queries) =>
        val children = queries.foldLeft(Seq(WorkingScope.apriori(incoming))) {
          case (previous, query) =>

            val nextQuery = apply(query, previous.last.outgoing, foreachIterVar = foreachIterVar)
            val intermediateOutgoing =
              if (nextQuery.result.isTableResult)
                incoming.replaceWith(nextQuery.result.getColumns.toSet)
              else
                incoming.replaceWith(unitVariables)

            val connectingQuery =
              StatementScope(
                astNode = Yield(ReturnItems(
                  FreeProjection,
                  nextQuery.result.getColumns.map(AliasedReturnItem(_))
                )(query.position))(query.position),
                incoming = previous.last.outgoing,
                referenced = References.connectOrDrop(nextQuery.result.getColumns, nextQuery.incoming.allSymbols),
                declared = Declarations(
                  constants = Seq.empty,
                  variables = nextQuery.result.getColumns
                ),
                outgoing = intermediateOutgoing
              )

            previous ++ Seq(nextQuery, connectingQuery)
        }.tail.dropRight(1)

        val referenced =
          Some(WorkingScope.referencedInChildren(children) intersectByTarget incoming.constantsAndVariables)
        incoming.resultScope(children.last.outgoing, children.last.result, children, referenced)
      case u: UnmappedUnion =>
        val rawChildren =
          Seq(u.lhs, u.rhs).map(q => apply(q, incoming, inImportingWith, foreachIterVar))
        val children = attachUnionMappingRefs(rawChildren, u.unionMappings)
        val unionVariables = u.unionMappings.map(_.unionVariable)
        val updatedVariables = children.head.result.getColumns.map(r =>
          unionVariables.find(_.name == r.name) match {
            case Some(v) => v
            case None    => r
          }
        )
        incoming
          .resultScope(
            children.head.outgoing,
            children.head.result replaceVariables updatedVariables,
            children,
            declared = Declarations(Seq.empty, unionVariables)
          )
      case u: ProjectingUnion =>
        val rawChildren =
          Seq(u.lhs, u.rhs).map(q => apply(q, incoming, inImportingWith, foreachIterVar))
        val children = attachUnionMappingRefs(rawChildren, u.unionMappings)
        val unionVariables = u.unionMappings.map(_.unionVariable)

        incoming
          .resultScope(
            children.head.outgoing,
            children.head.result replaceVariables unionVariables,
            children,
            declared = Declarations(Seq.empty, unionVariables)
          )
      case ConditionalQueryWhen(branches, defaultOpt) =>
        val allBranched = branches.appendedAll(defaultOpt)
        val branchIncoming = incoming.constantChildContext()
        val children = allBranched.map {
          case branch @ ConditionalQueryBranch(predicateOpt, query) =>
            val predicateScopeOpt =
              predicateOpt.map(predicate => pegExpression(predicate, branchIncoming))
            val queryScope = apply(query, branchIncoming, foreachIterVar = foreachIterVar)
            val branchChildren = Seq(predicateScopeOpt, Some(queryScope)).flatten
            val referenced =
              Some(
                WorkingScope.referencedInChildren(branchChildren) intersectByTarget branchIncoming.constantsAndVariables
              )
            implicit val astNode: ASTNode = branch
            branchIncoming.resultScope(queryScope.outgoing, queryScope.result, branchChildren, referenced)
        }
        incoming.resultScope(children.head.outgoing, children.head.result, children)
      case sq @ SingleQuery(clauses) if inImportingWith =>
        val partitioned = sq.partitionedClauses
        val initialUseOpt: Option[UseGraph] = partitioned.initialGraphSelection.collect { case u: UseGraph => u }
        val subsequentUseOpt: Option[UseGraph] = partitioned.subsequentGraphSelection.collect { case u: UseGraph => u }
        val impWithOpt: Option[With] = partitioned.importingWith
        val restClauses = partitioned.clausesExceptImportingWithAndLeadingGraphSelection

        val initialUseScopeOpt = initialUseOpt.map(uc => pegClause(uc, incoming, foreachIterVar))
        val impWithScopeOpt = impWithOpt.map(w => pegClause(w, incoming, foreachIterVar))

        val importedVariables: Set[LogicalVariable] = impWithOpt match {
          case None                                => unitVariables
          case Some(_) if sq.importColumns.isEmpty => incoming.allSymbols
          case Some(_) => incoming.allSymbols.filter(s => sq.importColumns.exists(_.name == s.name))
        }
        val queryIncoming = RegularContext(unitVariables, importedVariables, incoming.localCallables)
        val subsequentUseScopeOpt = subsequentUseOpt.map(uc => pegClause(uc, queryIncoming, foreachIterVar))

        val queryChildren = restClauses.scanLeft(WorkingScope.apriori(queryIncoming)) {
          case (previous, clause) => pegClause(clause, previous.outgoing, foreachIterVar) match {
              case ws @ StatementScope(_: CallClause, _, _, _, _, TableResult(_), _, _) => ws.copy(result = NoResult)
              case ws                                                                   => ws
            }
        }.tail

        val nestedUnderWith = subsequentUseScopeOpt.toSeq ++ queryChildren
        val children = impWithScopeOpt match {
          case Some(w) => initialUseScopeOpt.toSeq :+ w.withChildren(w.children ++ nestedUnderWith)
          case None    => initialUseScopeOpt.toSeq ++ queryChildren
        }
        val outgoing = queryChildren.lastOption.map(_.outgoing).getOrElse(queryIncoming)
        val result = queryChildren.lastOption.map(_.result).getOrElse(NoResult)
        val refSources =
          initialUseScopeOpt.toSeq ++ impWithScopeOpt.toSeq ++ subsequentUseScopeOpt.toSeq ++ queryChildren
        val referenced =
          Some(WorkingScope.referencedInChildren(refSources) intersectByTarget incoming.constantsAndVariables)
        incoming
          .resultScope(outgoing, result, children, referenced)(sq)
          .tagForCache(inImportingWith, foreachIterVar)

      case sq @ SingleQuery(clauses) =>
        if (clauses.size == 1 && clauses.head.isInstanceOf[UnresolvedCall]) {
          val child = pegClause(clauses.head, incoming, foreachIterVar)
          val referenced =
            Some(WorkingScope.referencedInChildren(Seq(child)) intersectByTarget incoming.constantsAndVariables)
          incoming.resultScope(child.outgoing, child.result, Seq(child), referenced)
        } else {
          val children = clauses.scanLeft(WorkingScope.apriori(incoming)) {
            case (previous, clause) => pegClause(clause, previous.outgoing, foreachIterVar) match {
                case ws @ StatementScope(_: CallClause, _, _, _, _, TableResult(_), _, _) => ws.copy(result = NoResult)
                case ws                                                                   => ws
              }
          }.tail
          // Alternatively, referenced can be computed by referencedInChildren minus "declaredInChildren"
          val referenced =
            Some(WorkingScope.referencedInChildren(children) intersectByTarget incoming.constantsAndVariables)
          // The following also wraps a single child in a parent scope.
          // Alternatively, we could simply forward a single child.
          incoming
            .resultScope(children.last.outgoing, children.last.result, children, referenced)(sq)
            .tagForCache(inImportingWith, foreachIterVar)
        }
      case TopLevelBraces(query, _) =>
        val inner = apply(query, incoming, foreachIterVar = foreachIterVar)
        incoming.resultScope(inner.outgoing, inner.result, Seq(inner))

      case command: AdministrationCommand => pegCommand(command, incoming)
      case command: SchemaCommand         => pegCommand(command, incoming)

      /**
       * To make match exhaustive
       */
      case _ => UnexpectedAstNodeScopingError(astNode, incoming)
    }
  }

  /**
   * Attach each `UnionMapping`'s `variableInLhs` / `variableInRhs` as a reference to the same-named
   * output column of its child branch. These go on the child scopes' `hiddenReferences` (not
   * the public `referenced`) so that:
   *   - `collectAllReferences` still surfaces them for `getSymbolGroups`.
   *   - The branch's public `referenced` is not polluted with mapping variables that don't come
   *     from outside the branch.
   */
  private def attachUnionMappingRefs(
    children: Seq[WorkingScope],
    unionMappings: Seq[UnionMapping]
  ): Seq[WorkingScope] = {
    val lhs = children.head
    val rhs = children(1)
    val lhsRefs = References.resolveByName(unionMappings.map(_.variableInLhs), lhs.result.getColumns)
    val rhsRefs = References.resolveByName(unionMappings.map(_.variableInRhs), rhs.result.getColumns)
    Seq(lhs.addHiddenReferences(lhsRefs), rhs.addHiddenReferences(rhsRefs))
  }
}
