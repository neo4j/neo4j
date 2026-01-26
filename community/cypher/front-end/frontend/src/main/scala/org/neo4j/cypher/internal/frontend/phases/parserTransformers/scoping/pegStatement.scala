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
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

object pegStatement {

  def apply(statement: Statement, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Statement](statement, incoming, applyUncached(_, _))
  }

  private def applyUncached(statement: Statement, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
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
            val (bodyChild, definitionResult) = lcd match {
              case LocalProcedureDefinition(_, _, outputSignatureOpt, body) =>
                val bodyChild = apply(body, bodyIncoming)
                val definitionResult = outputSignatureOpt.map(outputSignature =>
                  TableResult(outputSignature.map(lfs =>
                    Variable(lfs.name)(lfs.position, Variable.isIsolatedDefault).asInstanceOf[LogicalVariable]
                  ))
                ).getOrElse(bodyChild.result)
                (bodyChild, definitionResult)
              case LocalFunctionDefinition(_, _, _, body) =>
                val bodyChild = body match {
                  case QueryBody(query)           => apply(query, bodyIncoming)
                  case ExpressionBody(expression) => pegExpression(expression, bodyIncoming)
                }
                val definitionResult = ExpressionResult
                (bodyChild, definitionResult)
            }
            val localCallableScopeSignature = LocalCallableScopeSignature(lcd.name, definitionResult)
            val referenced = Some(Set.empty[LogicalVariable])
            val declared = Declarations.ofLocalCallable(localCallableScopeSignature)
            val outgoing = previous.outgoing.amendedWithLocalCallable(localCallableScopeSignature)
            implicit val astNode: ASTNode = lcd
            previous.outgoing.noResultScope(outgoing, Seq(bodyChild), referenced, declared)
        }.tail
        val queryIncoming = definitionsChildren.lastOption.map(definitionChild =>
          incoming.amendedWithLocalCallables(definitionChild.outgoing.localCallables)
        ).getOrElse(incoming)
        val queryChild = apply(query, queryIncoming)
        val outgoing = queryChild.outgoing.amendedWithLocalCallables(incoming.localCallables)
        val children = definitionsChildren :+ queryChild
        incoming.resultScope(outgoing, queryChild.result, children)
      case NextStatement(queries) =>
        val children = queries.foldLeft(Seq(WorkingScope.apriori(incoming))) {
          case (previous, query) =>

            val nextQuery = apply(query, previous.last.outgoing)
            val intermediateOutgoing =
              if (nextQuery.result.isTableResult)
                incoming.replaceWith(nextQuery.result.getColumns.toSet)
              else
                incoming.replaceWith(ScopeSurveyor.unitVariables)

            val connectingQuery =
              StatementScope(
                astNode = Yield(ReturnItems(
                  FreeProjection,
                  nextQuery.result.getColumns.map(AliasedReturnItem(_))
                )(query.position))(query.position),
                incoming = previous.last.outgoing,
                referenced = nextQuery.result.getColumns.toSet filter nextQuery.incoming.allSymbols,
                declared = Declarations(
                  constants = Seq.empty,
                  variables = nextQuery.result.getColumns
                ),
                outgoing = intermediateOutgoing
              )

            previous ++ Seq(nextQuery, connectingQuery)
        }.tail.dropRight(1)

        // Alternatively, referenced can be computed by referencedInChildren minus "declaredInChildren"
        val referenced =
          Some(WorkingScope.referencedInChildren(children) intersect incoming.constantsAndVariables)
        incoming.resultScope(children.last.outgoing, children.last.result, children, referenced)
      case u: Union =>
        val children = Seq(u.lhs, u.rhs).map(q => apply(q, incoming))
        incoming.resultScope(children.head.outgoing, children.head.result, children)
      case ConditionalQueryWhen(branches, defaultOpt) =>
        val allBranched = branches.appendedAll(defaultOpt)
        val branchIncoming = incoming.constantChildContext()
        val children = allBranched.map {
          case branch @ ConditionalQueryBranch(predicateOpt, query) =>
            val predicateScopeOpt =
              predicateOpt.map(predicate => pegExpression(predicate, branchIncoming))
            val queryScope = apply(query, branchIncoming)
            val branchChildren = Seq(predicateScopeOpt, Some(queryScope)).flatten
            val referenced =
              Some(WorkingScope.referencedInChildren(branchChildren) intersect branchIncoming.constantsAndVariables)
            implicit val astNode: ASTNode = branch
            branchIncoming.resultScope(queryScope.outgoing, queryScope.result, branchChildren, referenced)
        }
        incoming.resultScope(children.head.outgoing, children.head.result, children)
      case SingleQuery(clauses) =>
        if (clauses.size == 1 && clauses.head.isInstanceOf[UnresolvedCall]) {
          val child = pegClause(clauses.head, incoming)
          val referenced =
            Some(WorkingScope.referencedInChildren(Seq(child)) intersect incoming.constantsAndVariables)
          incoming.resultScope(child.outgoing, child.result, Seq(child), referenced)
        } else {
          val children = clauses.scanLeft(WorkingScope.apriori(incoming)) {
            case (previous, clause) => pegClause(clause, previous.outgoing) match {
                // adjusting for in-query calls to have no result
                case ws @ StatementScope(_: UnresolvedCall, _, _, _, _, TableResult(_), _) =>
                  ws.copy(result = NoResult)
                case ws => ws
              }
          }.tail
          // Alternatively, referenced can be computed by referencedInChildren minus "declaredInChildren"
          val referenced =
            Some(WorkingScope.referencedInChildren(children) intersect incoming.constantsAndVariables)
          // The following also wraps a single child in a parent scope.
          // Alternatively, we could simply forward a single child.
          incoming.resultScope(children.last.outgoing, children.last.result, children, referenced)
        }
      case TopLevelBraces(query, _) =>
        val inner = apply(query, incoming)
        incoming.resultScope(inner.outgoing, inner.result, Seq(inner))

      case command: AdministrationCommand => pegCommand(command, incoming)
      case command: SchemaCommand         => pegCommand(command, incoming)

      /**
       * To make match exhaustive
       */
      case _ => UnexpectedAstNodeScopingError(astNode, incoming)
    }
  }
}
