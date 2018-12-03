/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.opencypher.v9_0.ast.semantics.{SemanticTable => SemanticTableV3_5}
import org.opencypher.v9_0.{ast => astV3_5}
import org.opencypher.v9_0.ast.{semantics => semanticsV3_5}
import org.opencypher.v9_0.util.{InputPosition => InputPositionV3_5}
import org.opencypher.v9_0.{util => utilV3_5}
import org.opencypher.v9_0.expressions.{Expression => ExpressionV3_5}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable => SemanticTablev4_0}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression => Expressionv4_0}
import org.neo4j.cypher.internal.v4_0.{ast => astv4_0, expressions => expressionsv4_0, util => utilv4_0}

import scala.collection.mutable

object SemanticTableConverter {

  type ExpressionMapping4To5 = Map[(ExpressionV3_5, InputPositionV3_5), Expressionv4_0]

  // The semantic table needs to have the same objects as keys, therefore we cannot convert expressions again
  // but must use the already converted 3.5 instances
  def convertSemanticTable(table: SemanticTableV3_5, expressionMapping: ExpressionMapping4To5): SemanticTablev4_0 = {
    new SemanticTablev4_0(
      convert(table.types, expressionMapping),
      astv4_0.ASTAnnotationMap.empty,
      new mutable.HashMap[String, utilv4_0.LabelId],
      new mutable.HashMap[String, utilv4_0.PropertyKeyId],
      convert(table.resolvedRelTypeNames))
  }

  private def convert(types: astV3_5.ASTAnnotationMap[ExpressionV3_5, semanticsV3_5.ExpressionTypeInfo], expressionMapping: ExpressionMapping4To5):
  astv4_0.ASTAnnotationMap[expressionsv4_0.Expression, astv4_0.semantics.ExpressionTypeInfo] = {
    val result: Seq[(Expressionv4_0, ExpressionTypeInfo)] = types.toSeq.filter {
      case (exprV3_5, _) => expressionMapping.isDefinedAt((exprV3_5, exprV3_5.position))
    }.map {
      case (exprV3_5, typeInfoV3_5) => (expressionMapping((exprV3_5, exprV3_5.position)), convert(typeInfoV3_5))
    }
    astv4_0.ASTAnnotationMap(result:_*)
  }

  private def convert(resolvedRelTypeNames: mutable.Map[String, utilV3_5.RelTypeId]): mutable.Map[String, utilv4_0.RelTypeId] = {
    val res = new mutable.HashMap[String, utilv4_0.RelTypeId]
    resolvedRelTypeNames.foreach {
      case (key, r3_5) => res += ((key, utilv4_0.RelTypeId(r3_5.id)))
    }
    res
  }

  private def convert(typeInfoV3_5: semanticsV3_5.ExpressionTypeInfo): astv4_0.semantics.ExpressionTypeInfo = typeInfoV3_5 match {
    case semanticsV3_5.ExpressionTypeInfo(specified, expected) =>
      astv4_0.semantics.ExpressionTypeInfo(convert(specified), expected.map(convert))
  }

  private def convert(specified: utilV3_5.symbols.TypeSpec): utilv4_0.symbols.TypeSpec = {
      new utilv4_0.symbols.TypeSpec(specified.ranges.map(convert))
  }

  private def convert(range: utilV3_5.symbols.TypeRange): utilv4_0.symbols.TypeRange = range match {
    case utilV3_5.symbols.TypeRange(lower, upper) =>
      utilv4_0.symbols.TypeRange(convert(lower), upper.map(convert))
  }

  private def convert(cypherType: utilV3_5.symbols.CypherType): utilv4_0.symbols.CypherType = cypherType match {
    case utilV3_5.symbols.CTAny => utilv4_0.symbols.CTAny
    case utilV3_5.symbols.CTBoolean => utilv4_0.symbols.CTBoolean
    case utilV3_5.symbols.CTFloat => utilv4_0.symbols.CTFloat
    case utilV3_5.symbols.CTGeometry => utilv4_0.symbols.CTGeometry
    case utilV3_5.symbols.CTGraphRef => utilv4_0.symbols.CTGraphRef
    case utilV3_5.symbols.CTInteger => utilv4_0.symbols.CTInteger
    case utilV3_5.symbols.ListType(iteratedType) => utilv4_0.symbols.CTList(convert(iteratedType))
    case utilV3_5.symbols.CTMap => utilv4_0.symbols.CTMap
    case utilV3_5.symbols.CTNode => utilv4_0.symbols.CTNode
    case utilV3_5.symbols.CTNumber => utilv4_0.symbols.CTNumber
    case utilV3_5.symbols.CTPath => utilv4_0.symbols.CTPath
    case utilV3_5.symbols.CTPoint => utilv4_0.symbols.CTPoint
    case utilV3_5.symbols.CTRelationship => utilv4_0.symbols.CTRelationship
    case utilV3_5.symbols.CTString => utilv4_0.symbols.CTString
    case utilV3_5.symbols.CTDate => utilv4_0.symbols.CTDate
    case utilV3_5.symbols.CTDateTime => utilv4_0.symbols.CTDateTime
    case utilV3_5.symbols.CTTime => utilv4_0.symbols.CTTime
    case utilV3_5.symbols.CTLocalTime => utilv4_0.symbols.CTLocalTime
    case utilV3_5.symbols.CTLocalDateTime => utilv4_0.symbols.CTLocalDateTime
    case utilV3_5.symbols.CTDuration => utilv4_0.symbols.CTDuration
  }
}
