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
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.UsingBtreeIndexType
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedBtreeIndexSyntax
import org.neo4j.cypher.internal.util.DeprecatedCoercionOfListToBoolean
import org.neo4j.cypher.internal.util.DeprecatedCreateConstraintOnAssertSyntax
import org.neo4j.cypher.internal.util.DeprecatedCreateIndexSyntax
import org.neo4j.cypher.internal.util.DeprecatedCreatePropertyExistenceConstraintSyntax
import org.neo4j.cypher.internal.util.DeprecatedDefaultDatabaseSyntax
import org.neo4j.cypher.internal.util.DeprecatedDefaultGraphSyntax
import org.neo4j.cypher.internal.util.DeprecatedDropConstraintSyntax
import org.neo4j.cypher.internal.util.DeprecatedDropIndexSyntax
import org.neo4j.cypher.internal.util.DeprecatedHexLiteralSyntax
import org.neo4j.cypher.internal.util.DeprecatedOctalLiteralSyntax
import org.neo4j.cypher.internal.util.DeprecatedPatternExpressionOutsideExistsSyntax
import org.neo4j.cypher.internal.util.DeprecatedPeriodicCommit
import org.neo4j.cypher.internal.util.DeprecatedPointsComparison
import org.neo4j.cypher.internal.util.DeprecatedPropertyExistenceSyntax
import org.neo4j.cypher.internal.util.DeprecatedSelfReferenceToVariableInCreatePattern
import org.neo4j.cypher.internal.util.DeprecatedVarLengthBindingNotification
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTPoint

object Deprecations {

  def propertyOf(propertyKey: String): Expression => Expression =
    e => Property(e, PropertyKeyName(propertyKey)(e.position))(e.position)

  def renameFunctionTo(newName: String): FunctionInvocation => FunctionInvocation =
    f => f.copy(functionName = FunctionName(newName)(f.functionName.position))(f.position)

  def renameFunctionTo(newNamespace: Namespace, newName: String): FunctionInvocation => FunctionInvocation =
    f => f.copy(namespace = newNamespace, functionName = FunctionName(newName)(f.functionName.position))(f.position)

  case object syntacticallyDeprecatedFeaturesIn4_X extends SyntacticDeprecations {
    override val find: PartialFunction[Any, Deprecation] = {

      // old octal literal syntax, don't support underscores
      case p@SignedOctalIntegerLiteral(stringVal) if stringVal.charAt(stringVal.indexOf('0') + 1) != 'o' && stringVal.charAt(stringVal.indexOf('0') + 1) != '_' =>
        Deprecation(
          Some(Ref(p) -> SignedOctalIntegerLiteral(stringVal.patch(stringVal.indexOf('0') + 1, "o", 0))(p.position)),
          Some(DeprecatedOctalLiteralSyntax(p.position))
        )

      // old hex literal syntax
      case p@SignedHexIntegerLiteral(stringVal) if stringVal.charAt(stringVal.indexOf('0') + 1) == 'X' =>
        Deprecation(
          Some(Ref(p) -> SignedHexIntegerLiteral(stringVal.toLowerCase)(p.position)),
          Some(DeprecatedHexLiteralSyntax(p.position))
        )

      // timestamp
      case f@FunctionInvocation(namespace, FunctionName(name), _, _) if namespace.parts.isEmpty && name.equalsIgnoreCase("timestamp") =>
        Deprecation(
          Some(Ref(f) -> renameFunctionTo("datetime").andThen(propertyOf("epochMillis"))(f)),
          None
        )

      // var-length binding
      case p@RelationshipPattern(Some(variable), _, Some(_), _, _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedVarLengthBindingNotification(p.position, variable.name))
        )

      case i: ast.CreateIndexOldSyntax =>
        Deprecation(
          None,
          Some(DeprecatedCreateIndexSyntax(i.position))
        )

        // CREATE BTREE INDEX ...
      case i: ast.CreateBtreeNodeIndex =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(i.position))
        )

        // CREATE BTREE INDEX ...
      case i: ast.CreateBtreeRelationshipIndex =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(i.position))
        )

      // CREATE INDEX ... OPTIONS {<btree options>}
      case i: ast.CreateRangeNodeIndex if i.fromDefault && hasBtreeOptions(i.options) =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(i.position))
        )

      // CREATE INDEX ... OPTIONS {<btree options>}
      case i: ast.CreateRangeRelationshipIndex if i.fromDefault && hasBtreeOptions(i.options) =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(i.position))
        )

      case i: ast.DropIndex =>
        Deprecation(
          None,
          Some(DeprecatedDropIndexSyntax(i.position))
        )

      case c: ast.DropNodeKeyConstraint =>
        Deprecation(
          None,
          Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropUniquePropertyConstraint =>
        Deprecation(
          None,
          Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropNodePropertyExistenceConstraint =>
        Deprecation(
          None,
          Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropRelationshipPropertyExistenceConstraint =>
        Deprecation(
          None,
          Some(DeprecatedDropConstraintSyntax(c.position))
        )

      // CREATE CONSTRAINT ... OPTIONS {<btree options>}
      case c: ast.CreateNodeKeyConstraint if hasBtreeOptions(c.options) =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(c.position))
        )

      // CREATE CONSTRAINT ... OPTIONS {<btree options>}
      case c: ast.CreateUniquePropertyConstraint if hasBtreeOptions(c.options) =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(c.position))
        )

      // ASSERT EXISTS
      case c: ast.CreateNodePropertyExistenceConstraint if c.constraintVersion == ast.ConstraintVersion0 =>
        Deprecation(
          None,
          Some(DeprecatedCreatePropertyExistenceConstraintSyntax(c.position))
        )

      // ASSERT EXISTS
      case c: ast.CreateRelationshipPropertyExistenceConstraint if c.constraintVersion == ast.ConstraintVersion0 =>
        Deprecation(
          None,
          Some(DeprecatedCreatePropertyExistenceConstraintSyntax(c.position))
        )

      // CREATE CONSTRAINT ON ... ASSERT ...
      case c: ast.CreateNodePropertyExistenceConstraint if c.constraintVersion == ast.ConstraintVersion1 =>
        Deprecation(
          None,
          Some(DeprecatedCreateConstraintOnAssertSyntax(c.position))
        )

      // CREATE CONSTRAINT ON ... ASSERT ...
      case c: ast.CreateRelationshipPropertyExistenceConstraint if c.constraintVersion == ast.ConstraintVersion1 =>
        Deprecation(
          None,
          Some(DeprecatedCreateConstraintOnAssertSyntax(c.position))
        )

      // CREATE CONSTRAINT ON ... ASSERT ...
      case c: ast.CreateNodeKeyConstraint if c.constraintVersion == ast.ConstraintVersion0 =>
        Deprecation(
          None,
          Some(DeprecatedCreateConstraintOnAssertSyntax(c.position))
        )

      // CREATE CONSTRAINT ON ... ASSERT ...
      case c: ast.CreateUniquePropertyConstraint if c.constraintVersion == ast.ConstraintVersion0 =>
        Deprecation(
          None,
          Some(DeprecatedCreateConstraintOnAssertSyntax(c.position))
        )

      case e@Exists(_: Property | _: ContainerIndex) =>
        Deprecation(
          None,
          Some(DeprecatedPropertyExistenceSyntax(e.position))
        )

      case i: ast.ShowIndexesClause if i.indexType == ast.BtreeIndexes =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(i.position))
        )

      case c@ast.GrantPrivilege(ast.DatabasePrivilege(_, List(ast.DefaultDatabaseScope())), _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultDatabaseSyntax(c.position))
        )

      case c@ast.DenyPrivilege(ast.DatabasePrivilege(_, List(ast.DefaultDatabaseScope())), _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultDatabaseSyntax(c.position))
        )

      case c@ast.RevokePrivilege(ast.DatabasePrivilege(_, List(ast.DefaultDatabaseScope())), _, _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultDatabaseSyntax(c.position))
        )

      case c@ast.GrantPrivilege(ast.GraphPrivilege(_, List(ast.DefaultGraphScope())), _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultGraphSyntax(c.position))
        )

      case c@ast.DenyPrivilege(ast.GraphPrivilege(_, List(ast.DefaultGraphScope())), _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultGraphSyntax(c.position))
        )

      case c@ast.RevokePrivilege(ast.GraphPrivilege(_, List(ast.DefaultGraphScope())), _, _, _, _) =>
        Deprecation(
          None,
          Some(DeprecatedDefaultGraphSyntax(c.position))
        )

      case p: ast.PeriodicCommitHint =>
        Deprecation(
          None,
          Some(DeprecatedPeriodicCommit(p.position))
        )

      case h@ast.UsingIndexHint(_, _, _, _, UsingBtreeIndexType) =>
        Deprecation(
          None,
          Some(DeprecatedBtreeIndexSyntax(h.position))
        )
    }

    private def hasBtreeOptions(options: Options): Boolean = options match {
      case OptionsMap(opt) => opt.exists {
        case (key, value: StringLiteral) if key.equalsIgnoreCase("indexProvider") =>
          // Can't reach the GenericNativeIndexProvider and NativeLuceneFusionIndexProviderFactory30
          // so have to hardcode the btree providers instead
          value.value.equalsIgnoreCase("native-btree-1.0") || value.value.equalsIgnoreCase("lucene+native-3.0")

        case (key, value: MapExpression) if key.equalsIgnoreCase("indexConfig") =>
          // Can't reach the settings so have to hardcode them instead, only checks start of setting names
          //  spatial.cartesian.{min | max}
          //  spatial.cartesian-3d.{min | max}
          //  spatial.wgs-84.{min | max}
          //  spatial.wgs-84-3d.{min | max}
          val settings = value.items.map(_._1.name)
          settings.exists(name => name.toLowerCase.startsWith("spatial.cartesian") || name.toLowerCase.startsWith("spatial.wgs-84"))

        case _ => false
      }
      case _ => false
    }

    override def findWithContext(statement: ast.Statement): Set[Deprecation] = {
      def findExistsToIsNotNullReplacements(astNode: ASTNode): Set[Deprecation] = {
        astNode.treeFold[Set[Deprecation]](Set.empty) {
          case _: ast.Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors =>
            acc => TraverseChildren(acc)

          case e@Exists(p@(_: Property | _: ContainerIndex)) =>
            val deprecation = Deprecation(
              Some(Ref(e) -> IsNotNull(p)(e.position)),
              None
            )
            acc => SkipChildren(acc + deprecation)

          case _ =>
            acc => SkipChildren(acc)
        }
      }

      val replacementsFromExistsToIsNotNull = statement.treeFold[Set[Deprecation]](Set.empty) {
        case w: ast.Where =>
          val deprecations = findExistsToIsNotNullReplacements(w)
          acc => SkipChildren(acc ++ deprecations)

        case n: NodePattern =>
          val deprecations = n.predicate.fold(Set.empty[Deprecation])(findExistsToIsNotNullReplacements)
          acc => SkipChildren(acc ++ deprecations)

        case p: PatternComprehension =>
          val deprecations = p.predicate.fold(Set.empty[Deprecation])(findExistsToIsNotNullReplacements)
          acc => TraverseChildren(acc ++ deprecations)
      }

      replacementsFromExistsToIsNotNull
    }
  }

  case object semanticallyDeprecatedFeaturesIn4_X extends SemanticDeprecations {

    private def isExpectedTypeBoolean(semanticTable: SemanticTable, e: Expression) = semanticTable.types.get(e).exists(
      typeInfo => typeInfo.expected.fold(false)(CTBoolean.covariant.containsAll)
    )

    private def isPoint(semanticTable: SemanticTable, e: Expression) =
      semanticTable.types(e).actual == CTPoint.invariant

    private def isListCoercedToBoolean(semanticTable: SemanticTable, e: Expression): Boolean = semanticTable.types.get(e).exists(
      typeInfo =>
        CTList(CTAny).covariant.containsAll(typeInfo.specified) && isExpectedTypeBoolean(semanticTable, e)
    )

    private def hasSelfReferenceToVariableInPattern(pattern: Pattern, semanticTable: SemanticTable): Boolean = {
      val allSymbolDefinitions = semanticTable.recordedScopes(pattern).allSymbolDefinitions

      def findAllVariables(e: Any): Set[LogicalVariable] = e.findAllByClass[LogicalVariable].toSet
      def isDefinition(variable: LogicalVariable): Boolean = allSymbolDefinitions(variable.name).map(_.use).contains(Ref(variable))

      val (declaredVariables, referencedVariables) = pattern.treeFold[(Set[LogicalVariable], Set[LogicalVariable])]((Set.empty, Set.empty)) {
        case NodePattern(maybeVariable, _, _, maybeProperties, _)                  => acc => SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
        case RelationshipPattern(maybeVariable, _, _, maybeProperties, _, _, _) => acc => SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
        case NamedPatternPart(variable, _)                                      => acc => TraverseChildren((acc._1 + variable, acc._2))
      }

      (declaredVariables & referencedVariables).nonEmpty
    }

    override def find(semanticTable: SemanticTable): PartialFunction[Any, Deprecation] = {
      case e: Expression if isListCoercedToBoolean(semanticTable, e) =>
        Deprecation(
          None,
          Some(DeprecatedCoercionOfListToBoolean(e.position))
        )

      case x: InequalityExpression if isPoint(semanticTable, x.lhs) || isPoint(semanticTable, x.rhs) =>
        Deprecation(
          None,
          Some(DeprecatedPointsComparison(x.position))
        )

      // CREATE (a {prop:7})-[r:R]->(b {prop: a.prop})
      case Create(p: Pattern) if hasSelfReferenceToVariableInPattern(p, semanticTable) =>
        Deprecation(
          None,
          Some(DeprecatedSelfReferenceToVariableInCreatePattern(p.position))
        )
    }

    override def findWithContext(statement: ast.Statement,
                                 semanticTable: SemanticTable): Set[Deprecation] = {
      val deprecationsOfPatternExpressionsOutsideExists = statement.treeFold[Set[Deprecation]](Set.empty) {
        case Exists(_) =>
          // Don't look inside exists()
          deprecations => SkipChildren(deprecations)

        case p: PatternExpression if !isExpectedTypeBoolean(semanticTable, p) =>
          val deprecation = Deprecation(
            None,
            Some(DeprecatedPatternExpressionOutsideExistsSyntax(p.position))
          )
          deprecations => SkipChildren(deprecations + deprecation)
      }

      deprecationsOfPatternExpressionsOutsideExists
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
  def findWithContext(statement: ast.Statement, semanticTable: SemanticTable): Set[Deprecation] = Set.empty
}
