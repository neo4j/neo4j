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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetProperty
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.TextCreateIndex
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathLengthQuantifier
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.notification.DeprecatedImportingWithInSubqueryCall
import org.neo4j.cypher.internal.notification.DeprecatedKeywordVariableInWhenOperand
import org.neo4j.cypher.internal.notification.DeprecatedNodesOrRelationshipsInSetClauseNotification
import org.neo4j.cypher.internal.notification.DeprecatedPrecedenceOfLabelExpressionPredicate
import org.neo4j.cypher.internal.notification.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.notification.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.notification.DeprecatedWhereVariableInNodePattern
import org.neo4j.cypher.internal.notification.DeprecatedWhereVariableInRelationshipPattern
import org.neo4j.cypher.internal.notification.FixedLengthRelationshipInShortestPath
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

object Deprecations {

  case object SyntacticallyDeprecatedFeatures extends SyntacticDeprecations {

    val stringifier: ExpressionStringifier = ExpressionStringifier()

    override def find(version: CypherVersion): PartialFunction[Any, Deprecation] = Function.unlift {

      // legacy type separator -[:A|:B]->
      case rel @ RelationshipPattern(variable, Some(labelExpression), None, None, None, _)
        // this restriction is necessary because in all other cases, this is an error
        if variable.forall(variable => !AnonymousVariableNameGenerator.isNamed(variable.name)) &&
          !labelExpression.containsGpmSpecificRelTypeExpression &&
          labelExpression.folder.treeFindByClass[ColonDisjunction].nonEmpty =>
        val rewrittenExpression = labelExpression.replaceColonSyntax
        Some(Deprecation(
          Some(Ref(rel) -> rel.copy(labelExpression = Some(rewrittenExpression))(rel.position)),
          Some(DeprecatedRelTypeSeparatorNotification(
            labelExpression.folder.treeFindByClass[ColonDisjunction].get.position,
            s":${stringifier.stringifyLabelExpression(labelExpression)}",
            s":${stringifier.stringifyLabelExpression(rewrittenExpression)}"
          ))
        ))

      case NodePattern(Some(variable), None, Some(properties), None)
        if NodePattern.WhereVariableInNodePatterns.deprecatedIn(version) &&
          !variable.isIsolated && variable.name.equalsIgnoreCase("where") =>
        Some(Deprecation(
          None,
          Some(DeprecatedWhereVariableInNodePattern(variable.position, variable.name, stringifier(properties)))
        ))
      case RelationshipPattern(Some(variable), None, _, Some(properties), None, _)
        if RelationshipPattern.WhereVariableInRelationshipPatterns.deprecatedIn(version) &&
          !variable.isIsolated && variable.name.equalsIgnoreCase("where") =>
        Some(Deprecation(
          None,
          Some(DeprecatedWhereVariableInRelationshipPattern(variable.position, variable.name, stringifier(properties)))
        ))

      case Add(_, lep @ LabelExpressionPredicate(_, _))
        if LabelExpressionPredicate.UnparenthesizedLabelPredicateOnRhsOfAdd.deprecatedIn(version) &&
          !lep.isParenthesized =>
        Some(Deprecation(
          None,
          Some(DeprecatedPrecedenceOfLabelExpressionPredicate(lep.position, stringifier(lep)))
        ))

      case CaseExpression(Some(_), alternatives, _)
        if CaseExpression.KeywordVariablesInWhenOperand.deprecatedIn(version) =>
        alternatives.collectFirst {
          case (Equals(_, it @ IsTyped(variable: Variable, cypherType)), _)
            if !variable.isIsolated && variable.name.equalsIgnoreCase("is") && it.withDoubleColonOnly =>
            Deprecation(
              None,
              Some(DeprecatedKeywordVariableInWhenOperand(
                it.position,
                variable.name,
                s" :: ${cypherType.normalizedCypherTypeString()}"
              ))
            )
          case (Equals(_, add @ Add(variable: Variable, rhs)), _)
            if !variable.isIsolated && variable.name.equalsIgnoreCase("contains") =>
            Deprecation(
              None,
              Some(DeprecatedKeywordVariableInWhenOperand(
                add.position,
                variable.name,
                s" ${add.canonicalOperatorSymbol} ${stringifier(rhs)}"
              ))
            )
          case (Equals(_, subtract @ Subtract(variable: Variable, rhs)), _)
            if !variable.isIsolated && variable.name.equalsIgnoreCase("contains") =>
            Deprecation(
              None,
              Some(DeprecatedKeywordVariableInWhenOperand(
                subtract.position,
                variable.name,
                s" ${subtract.canonicalOperatorSymbol} ${stringifier(rhs)}"
              ))
            )
          case (Equals(_, containerIndex @ ContainerIndex(variable: Variable, index)), _)
            if !variable.isIsolated && variable.name.equalsIgnoreCase("in") =>
            Deprecation(
              None,
              Some(DeprecatedKeywordVariableInWhenOperand(
                containerIndex.position,
                variable.name,
                s"[${stringifier(index)}]"
              ))
            )
        }

      case s @ ShortestPathsPatternPart(
          relChain @ RelationshipChain(
            node1: NodePattern,
            relPat @ RelationshipPattern(variable, labelExpression, None, properties, predicate, direction),
            node2
          ),
          single
        ) =>
        val newRelPat = RelationshipPattern(
          variable,
          labelExpression,
          Some(Some(Range(
            Some(PathLengthQuantifier("1")(relPat.position)),
            Some(PathLengthQuantifier("1")(relPat.position))
          )(relPat.position))),
          properties,
          predicate,
          direction
        )(relPat.position)

        val replacement =
          ShortestPathsPatternPart(RelationshipChain(node1, newRelPat, node2)(relChain.position), single)(s.position)

        val deprecatedParameter = stringifier.patterns.apply(s)
        val replacementParameter = stringifier.patterns.apply(replacement)

        Some(Deprecation(
          None,
          Some(FixedLengthRelationshipInShortestPath(relPat.position, deprecatedParameter, replacementParameter))
        ))

      case c: CreateIndex if c.indexType == TextCreateIndex && hasOldTextIndexProvider(c.options) =>
        Some(Deprecation(
          None,
          Some(DeprecatedTextIndexProvider(c.position))
        ))

      case _ => None
    }

    private def hasOldTextIndexProvider(options: Options): Boolean = options match {
      case OptionsMap(opt) => opt.exists {
          case (key, value: StringLiteral) if key.equalsIgnoreCase("indexProvider") =>
            // Can't reach the TextIndexProvider
            // so have to hardcode the old text provider instead
            value.value.equalsIgnoreCase("text-1.0")

          case _ => false
        }
      case _ => false
    }
  }

  private def functionInvocationForSetProperties(s: SetProperty, e: Variable): FunctionInvocation = {
    FunctionInvocation(
      functionName = FunctionName(Namespace(List())(e.position), "properties")(e.position),
      distinct = false,
      args = Vector(e),
      maybeLocalFunction = None
    )(s.position)
  }

  // add new semantically deprecated features here
  case object SemanticallyDeprecatedFeatures extends SemanticDeprecations {

    override def find(version: CypherVersion, semanticTable: SemanticTable): PartialFunction[Any, Deprecation] =
      Function.unlift {
        case s @ SetExactPropertiesFromMapItem(lhs: Variable, rhs: Variable, false)
          if semanticTable.typeFor(rhs).isAnyOf(CTNode, CTRelationship) =>
          Some(Deprecation(
            Some(Ref(s) -> s.copy(expression = functionInvocationForSetProperties(s, rhs))(s.position)),
            Some(DeprecatedNodesOrRelationshipsInSetClauseNotification(
              rhs.position,
              s"SET ${lhs.name} = ${rhs.name}",
              s"SET ${lhs.name} = properties(${rhs.name})"
            ))
          ))
        case s @ SetIncludingPropertiesFromMapItem(lhs: Variable, rhs: Variable, false)
          if semanticTable.typeFor(rhs).isAnyOf(CTNode, CTRelationship) =>
          Some(Deprecation(
            Some(Ref(s) -> s.copy(expression = functionInvocationForSetProperties(s, rhs))(s.position)),
            Some(DeprecatedNodesOrRelationshipsInSetClauseNotification(
              rhs.position,
              s"SET ${lhs.name} += ${rhs.name}",
              s"SET ${lhs.name} += properties(${rhs.name})"
            ))
          ))

        case c @ ImportingWithSubqueryCall(innerQuery, _, optional) =>
          def includesExisting(q: Query): Boolean = {
            q match {
              case sq: SingleQuery => sq.partitionedClauses.importingWith.exists(w => w.returnItems.includeExisting)
              case un: Union =>
                un.rhs.singleQuery.partitionedClauses.importingWith.exists(w => w.returnItems.includeExisting) ||
                includesExisting(un.lhs)
              case wh: ConditionalQueryWhen =>
                wh.branches.exists(b => includesExisting(b.query)) || wh.default.exists(d => includesExisting(d.query))
              case tlb: TopLevelBraces => includesExisting(tlb.query)
              case nxt: NextStatement  => nxt.queries.exists(includesExisting)
              case QueryWithLocalDefinitions(definitions, query) => includesExisting(query) || definitions.exists {
                  case LocalProcedureDefinition(_, _, _, body)             => includesExisting(body)
                  case LocalFunctionDefinition(_, _, _, QueryBody(body))   => includesExisting(body)
                  case LocalFunctionDefinition(_, _, _, ExpressionBody(_)) => false
                }
            }
          }

          val subqueryType = if (optional) "OPTIONAL CALL" else "CALL"

          val importing = if (innerQuery.isCorrelated) {
            if (includesExisting(innerQuery)) "*" else innerQuery.importColumns.map(_.name).distinct.mkString(", ")
          } else ""

          Some(Deprecation(
            None,
            Some(DeprecatedImportingWithInSubqueryCall(c.position, subqueryType, importing))
          ))

        case _ => None
      }
  }
}

/**
 * One deprecation.
 *
 * This class holds both the ability to replace a part of the AST with the preferred non-deprecated variant, and
 * the ability to generate an optional notification to the user that they are using a deprecated feature.
 *
 * @param replacement  an optional replacement tuple with the ASTNode to be replaced and its replacement.
 * @param notification optional appropriate deprecation notification
 */
case class Deprecation(replacement: Option[(Ref[ASTNode], ASTNode)], notification: Option[InternalNotification])

sealed trait Deprecations

trait SyntacticDeprecations extends Deprecations {
  def find(version: CypherVersion): PartialFunction[Any, Deprecation]
  def findWithContext(statement: ast.Statement): Set[Deprecation] = Set.empty
}

trait SemanticDeprecations extends Deprecations {
  def find(version: CypherVersion, semanticTable: SemanticTable): PartialFunction[Any, Deprecation]
}
