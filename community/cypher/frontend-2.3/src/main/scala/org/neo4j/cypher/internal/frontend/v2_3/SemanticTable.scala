/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.ast.{Identifier, ASTNode, Expression, ASTAnnotationMap}
import org.neo4j.cypher.internal.frontend.v2_3.symbols.TypeSpec

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
      case (Identifier(name), typ) if name == s => typ.specified
    }.reduce(_ & _)

    if (reducedType.isEmpty)
      throw new InternalException(s"This semantic table contains conflicting type information for identifier $s")

    reducedType
  } catch {
    case e: UnsupportedOperationException =>
      throw new InternalException(s"Did not find any type information for identifier $s", e)
  }

  def isNode(expr: String) = getTypeFor(expr) == symbols.CTNode.invariant

  def isRelationship(expr: String) = getTypeFor(expr) == symbols.CTRelationship.invariant

  def isNode(expr: Identifier) = types(expr).specified == symbols.CTNode.invariant

  def isRelationship(expr: Identifier) = types(expr).specified == symbols.CTRelationship.invariant

  def addNode(expr: Identifier) =
    copy(types = types.updated(expr, ExpressionTypeInfo(symbols.CTNode.invariant, None)))

  def addRelationship(expr: Identifier) =
    copy(types = types.updated(expr, ExpressionTypeInfo(symbols.CTRelationship.invariant, None)))

  def replaceKeys(replacements: (Identifier, Identifier)*): SemanticTable =
    copy(types = types.replaceKeys(replacements: _*), recordedScopes = recordedScopes.replaceKeys(replacements: _*))

  def symbolDefinition(identifier: Identifier) =
    recordedScopes(identifier).symbolTable(identifier.name).definition

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

