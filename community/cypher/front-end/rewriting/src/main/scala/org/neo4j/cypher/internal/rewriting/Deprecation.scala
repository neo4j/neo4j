/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetProperty
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedNodesOrRelationshipsInSetClauseNotification
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.util.FixedLengthRelationshipInShortestPath
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.UnionReturnItemsInDifferentOrder
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

object Deprecations {

  case object syntacticallyDeprecatedFeatures extends SyntacticDeprecations {

    val stringifier: ExpressionStringifier = ExpressionStringifier()

    override val find: PartialFunction[Any, Deprecation] = {

      // legacy type separator -[:A|:B]->
      case rel @ RelationshipPattern(variable, Some(labelExpression), None, None, None, _)
        // this restriction is necessary because in all other cases, this is an error
        if variable.forall(variable => !AnonymousVariableNameGenerator.isNamed(variable.name)) &&
          !labelExpression.containsGpmSpecificRelTypeExpression &&
          labelExpression.folder.treeFindByClass[ColonDisjunction].nonEmpty =>
        val rewrittenExpression = labelExpression.replaceColonSyntax
        Deprecation(
          Some(Ref(rel) -> rel.copy(labelExpression = Some(rewrittenExpression))(rel.position)),
          Some(DeprecatedRelTypeSeparatorNotification(
            labelExpression.folder.treeFindByClass[ColonDisjunction].get.position,
            s":${stringifier.stringifyLabelExpression(rewrittenExpression)}"
          ))
        )
      case UnionAll(lhs: Query, rhs: SingleQuery) if unionReturnItemsInDifferentOrder(lhs, rhs) =>
        Deprecation(
          None,
          Some(UnionReturnItemsInDifferentOrder(lhs.position))
        )
      case UnionDistinct(lhs: Query, rhs: SingleQuery) if unionReturnItemsInDifferentOrder(lhs, rhs) =>
        Deprecation(
          None,
          Some(UnionReturnItemsInDifferentOrder(lhs.position))
        )

      case ShortestPathsPatternPart(
          RelationshipChain(_: NodePattern, relPat @ RelationshipPattern(_, _, None, _, _, _), _),
          _
        ) =>
        Deprecation(
          None,
          Some(FixedLengthRelationshipInShortestPath(relPat.position))
        )

      case c @ CreateDatabase(nn @ NamespacedName(_, Some(_)), _, _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDatabaseNameNotification(nn.toString, Some(c.position)))
        )

      case c: CreateTextNodeIndex if hasOldTextIndexProvider(c.options) =>
        Deprecation(
          None,
          Some(DeprecatedTextIndexProvider(c.position))
        )
      case c: CreateTextRelationshipIndex if hasOldTextIndexProvider(c.options) =>
        Deprecation(
          None,
          Some(DeprecatedTextIndexProvider(c.position))
        )
      case f @ FunctionInvocation(namespace, FunctionName(name), _, _)
        if namespace.parts.isEmpty && name.equalsIgnoreCase("id") =>
        Deprecation(
          None,
          Some(DeprecatedFunctionNotification(f.position, "id", null))
        )
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
      namespace = Namespace(List())(e.position),
      functionName = FunctionName("properties")(e.position),
      distinct = false,
      args = Vector(e)
    )(s.position)
  }

  private def isNodeOrRelationship(v: Variable, semanticTable: SemanticTable): Boolean = {
    val actualType = semanticTable.getActualTypeFor(v)
    actualType.equals(CTNode.invariant) || actualType.equals(CTRelationship.invariant)
  }

  private def unionReturnItemsInDifferentOrder(lhs: Query, rhs: SingleQuery): Boolean = {
    rhs.returnColumns.nonEmpty && lhs.returnColumns.nonEmpty &&
    !rhs.returnColumns.map(v => v.name).equals(lhs.returnColumns.map(v => v.name))
  }

  // add new semantically deprecated features here
  case object semanticallyDeprecatedFeatures extends SemanticDeprecations {

    // Returns the set of variables that are defined in a `CREATE` and then used in the same `CREATE` for property read
    // E.g. `CREATE (a {prop: 5}), (b {prop: a.prop})
    def propertyUsageOfNewVariable(pattern: Pattern, semanticTable: SemanticTable): Set[LogicalVariable] = {
      val allSymbolDefinitions = semanticTable.recordedScopes(pattern).allSymbolDefinitions

      def findAllVariables(e: Option[Expression]): Set[LogicalVariable] = e.folder.findAllByClass[LogicalVariable].toSet
      def isDefinition(variable: LogicalVariable): Boolean =
        allSymbolDefinitions(variable.name).map(_.use).contains(Ref(variable))

      val (declaredVariables, referencedVariables) =
        pattern.folder.treeFold[(Set[LogicalVariable], Set[LogicalVariable])]((Set.empty, Set.empty)) {
          case NodePattern(maybeVariable, _, maybeProperties, _) => acc =>
              SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
          case RelationshipPattern(maybeVariable, _, _, maybeProperties, _, _) => acc =>
              SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
          case NamedPatternPart(variable, _) => acc =>
              TraverseChildren((acc._1 + variable, acc._2))
        }
      referencedVariables.intersect(declaredVariables)
    }

    override def find(semanticTable: SemanticTable): PartialFunction[Any, Deprecation] = Function.unlift {
      case s @ SetExactPropertiesFromMapItem(_, e: Variable) if isNodeOrRelationship(e, semanticTable) =>
        Some(Deprecation(
          Some(Ref(s) -> s.copy(expression = functionInvocationForSetProperties(s, e))(s.position)),
          Some(DeprecatedNodesOrRelationshipsInSetClauseNotification(e.position))
        ))
      case s @ SetIncludingPropertiesFromMapItem(_, e: Variable) if isNodeOrRelationship(e, semanticTable) =>
        Some(Deprecation(
          Some(Ref(s) -> s.copy(expression = functionInvocationForSetProperties(s, e))(s.position)),
          Some(DeprecatedNodesOrRelationshipsInSetClauseNotification(e.position))
        ))

      case Create(pattern) =>
        propertyUsageOfNewVariable(pattern, semanticTable).collectFirst { e =>
          Deprecation(None, Some(DeprecatedPropertyReferenceInCreate(e.position, e.name)))
        }

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
  def find: PartialFunction[Any, Deprecation]
  def findWithContext(statement: ast.Statement): Set[Deprecation] = Set.empty
}

trait SemanticDeprecations extends Deprecations {
  def find(semanticTable: SemanticTable): PartialFunction[Any, Deprecation]
}
