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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.ASTAnnotationMap.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.ast.semantics.SemanticTable.TypeGetter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.runtime.ScalaRunTime

object SemanticTable {

  /**
   * Provides convenience functions for some optionally missing type information.
   */
  case class TypeGetter(typeInfo: Option[TypeSpec]) {

    /**
     * Returns true iff the typeInfo is available and exactly contains the provided cypher type.
     */
    def is(cypherType: CypherType): Boolean =
      typeInfo.contains(cypherType.invariant)

    /**
     * Returns true iff the typeInfo is available and exactly contains one of the provided cypher types.
     */
    def isAnyOf(cypherTypes: CypherType*): Boolean =
      cypherTypes.exists(ct => typeInfo.contains(ct.invariant))

    /**
     * Returns true iff
     * either the typeInfo is available and could contain any of the provided cypher types,
     * or if the typeInfo is not available.
     */
    def couldBe(cypherTypes: CypherType*): Boolean =
      typeInfo.fold(true)(_.containsAny(cypherTypes: _*))
  }
}

trait TokenTable {
  def id(labelName: LabelName): Option[LabelId]
  def id(propertyKeyName: PropertyKeyName): Option[PropertyKeyId]
  def id(resolvedRelTypeName: RelTypeName): Option[RelTypeId]
}

final case class SemanticTable(
  types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = ASTAnnotationMap.empty,
  recordedScopes: ASTAnnotationMap[ASTNode, Scope] = ASTAnnotationMap.empty,
  resolvedLabelNames: Map[String, LabelId] = Map.empty,
  resolvedPropertyKeyNames: Map[String, PropertyKeyId] = Map.empty,
  resolvedRelTypeNames: Map[String, RelTypeId] = Map.empty
) extends TokenTable {

  private val hashCodeLazy: LazyVal[Int] = LazyVal(ScalaRunTime._hashCode(this))
  override def hashCode: Int = hashCodeLazy.value

  override def id(labelName: LabelName): Option[LabelId] = resolvedLabelNames.get(labelName.name)

  override def id(propertyKeyName: PropertyKeyName): Option[PropertyKeyId] =
    resolvedPropertyKeyNames.get(propertyKeyName.name)

  override def id(resolvedRelTypeName: RelTypeName): Option[RelTypeId] =
    resolvedRelTypeNames.get(resolvedRelTypeName.name)

  def symbolDefinition(variable: Variable): SymbolUse =
    recordedScopes(variable).symbolTable(variable.name).definition

  // Type getters

  /**
   * Returns the actual type of the specified variable name if it exists and has no conflicting type information, else none.
   */
  def typeFor(variableName: String): TypeGetter = {
    val matchedTypes = types.collect {
      case (PositionedNode(Variable(name)), typ) if name == variableName => typ.actual
    }

    val typeInfo = if (matchedTypes.nonEmpty) {
      Some(matchedTypes.reduce(_ & _))
        .filterNot(_.isEmpty) // Ignores cases when semantic table contains conflicting type information
    } else {
      None
    }
    TypeGetter(typeInfo)
  }

  /**
   * Returns the actual type of the given expression if it exists, else none.
   */
  def typeFor(expr: Expression): TypeGetter =
    TypeGetter(types.get(expr).map(_.actual))

// Copy methods

  def addNode(expr: Variable): SemanticTable =
    addTypeInfo(expr, CTNode.invariant)

  def addRelationship(expr: Variable): SemanticTable =
    addTypeInfo(expr, CTRelationship.invariant)

  def addTypeInfoCTAny(expr: Expression): SemanticTable =
    addTypeInfo(expr, CTAny.invariant)

  def addTypeInfo(expr: Expression, typeSpec: TypeSpec): SemanticTable =
    copy(types = types.updated(expr, ExpressionTypeInfo(typeSpec, None)))

  def replaceExpressions(rewriter: Rewriter): SemanticTable = {
    val replacements = types.keys.toIndexedSeq.map { keyExpression =>
      keyExpression -> keyExpression.endoRewrite(rewriter)
    }
    copy(types = types.replaceKeys(replacements: _*), recordedScopes = recordedScopes.replaceKeys(replacements: _*))
  }

  def addResolvedLabelName(name: String, labelId: LabelId): SemanticTable =
    copy(resolvedLabelNames = resolvedLabelNames + (name -> labelId))

  def addResolvedLabelNames(names: IterableOnce[(String, LabelId)]): SemanticTable =
    copy(resolvedLabelNames = resolvedLabelNames ++ names)

  def addResolvedRelTypeName(name: String, relTypeId: RelTypeId): SemanticTable =
    copy(resolvedRelTypeNames = resolvedRelTypeNames + (name -> relTypeId))

  def addResolvedPropertyKeyName(name: String, propertyKeyId: PropertyKeyId): SemanticTable =
    copy(resolvedPropertyKeyNames = resolvedPropertyKeyNames + (name -> propertyKeyId))
}

final case class CachableSemanticTable(
  private val listOfNodeExpressions: Set[PositionedNode[Expression]],
  private val listOfRelExpressions: Set[PositionedNode[Expression]],

  // TODO Remove these from the cache, kernel keeps an in memory map of tokens anyway.
  private val resolvedLabelNames: Map[String, LabelId] = Map.empty,
  private val resolvedPropertyKeyNames: Map[String, PropertyKeyId] = Map.empty,
  private val resolvedRelTypeNames: Map[String, RelTypeId] = Map.empty
) extends TokenTable {

  override def id(label: LabelName): Option[LabelId] = resolvedLabelNames.get(label.name)
  override def id(property: PropertyKeyName): Option[PropertyKeyId] = resolvedPropertyKeyNames.get(property.name)
  override def id(relType: RelTypeName): Option[RelTypeId] = resolvedRelTypeNames.get(relType.name)

  def isListOfNodes(expression: Expression): Boolean =
    listOfNodeExpressions.contains(PositionedNode(expression))

  def isListOfRelationships(expression: Expression): Boolean =
    listOfRelExpressions.contains(PositionedNode(expression))
}

object CachableSemanticTable {

  def apply(table: SemanticTable): CachableSemanticTable = {
    CachableSemanticTable(
      listOfNodeExpressions = table.types.keys.filter(e => table.typeFor(e.node).is(CTList(CTNode))).toSet,
      listOfRelExpressions = table.types.keys.filter(e => table.typeFor(e.node).is(CTList(CTRelationship))).toSet,
      resolvedLabelNames = table.resolvedLabelNames,
      resolvedPropertyKeyNames = table.resolvedPropertyKeyNames,
      resolvedRelTypeNames = table.resolvedRelTypeNames
    )
  }
}
