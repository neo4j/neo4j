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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.collection.mutable

object SemanticTable {
  def apply(types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = ASTAnnotationMap.empty,
            recordedScopes: ASTAnnotationMap[ASTNode, Scope] = ASTAnnotationMap.empty) =
    new SemanticTable(types, recordedScopes)
}

class SemanticTable(
                     val types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = ASTAnnotationMap.empty,
                     val recordedScopes: ASTAnnotationMap[ASTNode, Scope] = ASTAnnotationMap.empty,
                     val resolvedLabelNames: mutable.Map[String, LabelId] = new mutable.HashMap[String, LabelId],
                     val resolvedPropertyKeyNames: mutable.Map[String, PropertyKeyId] = new mutable.HashMap[String, PropertyKeyId],
                     val resolvedRelTypeNames: mutable.Map[String, RelTypeId] = new mutable.HashMap[String, RelTypeId]
  ) extends Cloneable {

  def getTypeFor(s: String): TypeSpec = try {
    val reducedType = types.collect {
      case (Variable(name), typ) if name == s => typ.specified
    }.reduce(_ & _)

    if (reducedType.isEmpty)
      throw new IllegalStateException(s"This semantic table contains conflicting type information for variable $s")

    reducedType
  } catch {
    case e: UnsupportedOperationException =>
      throw new IllegalStateException(s"Did not find any type information for variable $s", e)
  }

  def getActualTypeFor(expr: Expression): TypeSpec =
    types.getOrElse(expr, throw new IllegalStateException(s"Did not find any type information for expression $expr")).actual

  /**
   * Returns the actual type of the specified variable name if it exists and has no conflicting type information, else none.
   */
  def getOptionalActualTypeFor(variableName: String): Option[TypeSpec] = {
    val matchedTypes = types.collect {
      case (Variable(name), typ) if name == variableName => typ.actual
    }

    if (matchedTypes.nonEmpty) {
      Some(matchedTypes.reduce(_ intersect _))
        .filterNot(_.isEmpty) // Ignores cases when semantic table contains conflicting type information
    } else {
      None
    }
  }

  def containsNode(expr: String): Boolean = types.exists {
    case (v@Variable(name), _) => name == expr && isNode(v) // NOTE: Profiling showed that checking node type last is better
    case _ => false
  }

  def id(labelName:LabelName):Option[LabelId] = resolvedLabelNames.get(labelName.name)

  def id(propertyKeyName:PropertyKeyName):Option[PropertyKeyId] = resolvedPropertyKeyNames.get(propertyKeyName.name)

  def id(resolvedRelTypeName:RelTypeName):Option[RelTypeId] = resolvedRelTypeNames.get(resolvedRelTypeName.name)

  def seen(expression: Expression): Boolean = types.contains(expression)

  def isNode(expr: String): Boolean = getTypeFor(expr) == CTNode.invariant

  /**
   * Returns true if the specified variable exists, is a node and has no conflicting type information.
   */
  def isNodeNoFail(variableName: String): Boolean = getOptionalActualTypeFor(variableName).contains(CTNode.invariant)

  def isRelationship(expr: String): Boolean = getTypeFor(expr) == CTRelationship.invariant

  /**
   * Returns true if the specified variable exists, is a relationship and has no conflicting type information.
   */
  def isRelationshipNoFail(variableName: String): Boolean = getOptionalActualTypeFor(variableName).contains(CTRelationship.invariant)

  def isRelationshipCollection(expr: String): Boolean = getTypeFor(expr) == CTList(CTRelationship).invariant

  def isNodeCollection(expr: String): Boolean = getTypeFor(expr) == CTList(CTNode).invariant

  def isNode(expr: Expression): Boolean = types(expr).specified == CTNode.invariant

  /**
   * Same as isNode, but will simply return false if no semantic information is available instead of failing.
   */
  def isNodeNoFail(expr: Expression): Boolean = types.get(expr).map(_.specified).contains(CTNode.invariant)

  def isRelationship(expr: Expression): Boolean = types(expr).specified == CTRelationship.invariant

  /**
   * Same as isRelationship, but will simply return false if no semantic information is available instead of failing.
   */
  def isRelationshipNoFail(expr: Expression): Boolean = types.get(expr).map(_.specified).contains(CTRelationship.invariant)

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

  def replaceNodes(replacements: (ASTNode, ASTNode)*): SemanticTable =
    copy(recordedScopes = recordedScopes.replaceKeys(replacements: _*))

  def symbolDefinition(variable: Variable): SymbolUse =
    recordedScopes(variable).symbolTable(variable.name).definition

  override def clone(): SemanticTable = copy()

  def copy(
            types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = types,
            recordedScopes: ASTAnnotationMap[ASTNode, Scope] = recordedScopes,
            resolvedLabelIds: mutable.Map[String, LabelId] = resolvedLabelNames,
            resolvedPropertyKeyNames: mutable.Map[String, PropertyKeyId] = resolvedPropertyKeyNames,
            resolvedRelTypeNames: mutable.Map[String, RelTypeId] = resolvedRelTypeNames
  ) =
    new SemanticTable(types, recordedScopes, resolvedLabelIds.clone(), resolvedPropertyKeyNames.clone(), resolvedRelTypeNames.clone())
}
