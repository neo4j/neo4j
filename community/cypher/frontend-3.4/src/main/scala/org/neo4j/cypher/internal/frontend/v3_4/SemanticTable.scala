/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ast.{ASTAnnotationMap, ASTNode, Expression, Variable}
import org.neo4j.cypher.internal.frontend.v3_4.symbols.TypeSpec

import scala.collection.mutable

object SemanticTable {
  def apply(types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = ASTAnnotationMap.empty,
            recordedScopes: ASTAnnotationMap[ASTNode, Scope] = ASTAnnotationMap.empty) =
    new SemanticTable(types, recordedScopes)
}

class SemanticTable(
    val types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = ASTAnnotationMap.empty,
    val recordedScopes: ASTAnnotationMap[ASTNode, Scope] = ASTAnnotationMap.empty,
    val resolvedLabelIds: mutable.Map[String, LabelId] = new mutable.HashMap[String, LabelId],
    val resolvedPropertyKeyNames: mutable.Map[String, PropertyKeyId] = new mutable.HashMap[String, PropertyKeyId],
    val resolvedRelTypeNames: mutable.Map[String, RelTypeId] = new mutable.HashMap[String, RelTypeId]
  ) extends Cloneable {

  def getTypeFor(s: String): TypeSpec = try {
    val reducedType = types.collect {
      case (Variable(name), typ) if name == s => typ.specified
    }.reduce(_ & _)

    if (reducedType.isEmpty)
      throw new InternalException(s"This semantic table contains conflicting type information for variable $s")

    reducedType
  } catch {
    case e: UnsupportedOperationException =>
      throw new InternalException(s"Did not find any type information for variable $s", e)
  }

  def containsNode(expr: String): Boolean = types.exists {
    case (v@Variable(name), _) => name == expr && isNode(v) // NOTE: Profiling showed that checking node type last is better
    case _ => false
  }

  def seen(expression: Expression) = types.contains(expression)

  def isNode(expr: String) = getTypeFor(expr) == symbols.CTNode.invariant

  def isRelationship(expr: String) = getTypeFor(expr) == symbols.CTRelationship.invariant

  def isRelationshipCollection(expr: String) = getTypeFor(expr) == symbols.CTList(symbols.CTRelationship).invariant

  def isNodeCollection(expr: String) = getTypeFor(expr) == symbols.CTList(symbols.CTNode).invariant

  def isNode(expr: Variable) = types(expr).specified == symbols.CTNode.invariant

  def isRelationship(expr: Variable) = types(expr).specified == symbols.CTRelationship.invariant

  def addNode(expr: Variable) =
    copy(types = types.updated(expr, ExpressionTypeInfo(symbols.CTNode.invariant, None)))

  def addRelationship(expr: Variable) =
    copy(types = types.updated(expr, ExpressionTypeInfo(symbols.CTRelationship.invariant, None)))

  def replaceVariables(replacements: (Variable, Variable)*): SemanticTable =
    copy(types = types.replaceKeys(replacements: _*), recordedScopes = recordedScopes.replaceKeys(replacements: _*))

  def replaceNodes(replacements: (ASTNode, ASTNode)*): SemanticTable =
    copy(recordedScopes = recordedScopes.replaceKeys(replacements: _*))

  def symbolDefinition(variable: Variable) =
    recordedScopes(variable).symbolTable(variable.name).definition

  override def clone() = copy()

  def copy(
    types: ASTAnnotationMap[Expression, ExpressionTypeInfo] = types,
    recordedScopes: ASTAnnotationMap[ASTNode, Scope] = recordedScopes,
    resolvedLabelIds: mutable.Map[String, LabelId] = resolvedLabelIds,
    resolvedPropertyKeyNames: mutable.Map[String, PropertyKeyId] = resolvedPropertyKeyNames,
    resolvedRelTypeNames: mutable.Map[String, RelTypeId] = resolvedRelTypeNames
  ) =
    new SemanticTable(types, recordedScopes, resolvedLabelIds.clone(), resolvedPropertyKeyNames.clone(), resolvedRelTypeNames.clone())
}

