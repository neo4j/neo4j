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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticTable => SemanticTableV3_4}
import org.neo4j.cypher.internal.frontend.v3_4.{ast => astV3_4, semantics => semanticsV3_4}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition => InputPositionV3_4}
import org.neo4j.cypher.internal.util.{v3_4 => utilV3_4}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable => SemanticTablev4_0}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression => Expressionv4_0}
import org.neo4j.cypher.internal.v4_0.{ast => astv4_0, expressions => expressionsv4_0, util => utilv4_0}

import scala.collection.mutable

object SemanticTableConverter {

  type ExpressionMapping4To5 = Map[(ExpressionV3_4, InputPositionV3_4), Expressionv4_0]

  // The semantic table needs to have the same objects as keys, therefore we cannot convert expressions again
  // but must use the already converted 3.4 instances
  def convertSemanticTable(table: SemanticTableV3_4, expressionMapping: ExpressionMapping4To5): SemanticTablev4_0 = {
    new SemanticTablev4_0(
      convert(table.types, expressionMapping),
      astv4_0.ASTAnnotationMap.empty,
      new mutable.HashMap[String, utilv4_0.LabelId],
      new mutable.HashMap[String, utilv4_0.PropertyKeyId],
      convert(table.resolvedRelTypeNames))
  }

  private def convert(types: astV3_4.ASTAnnotationMap[ExpressionV3_4, semanticsV3_4.ExpressionTypeInfo], expressionMapping: ExpressionMapping4To5):
  astv4_0.ASTAnnotationMap[expressionsv4_0.Expression, astv4_0.semantics.ExpressionTypeInfo] = {
    val result: Seq[(Expressionv4_0, ExpressionTypeInfo)] = types.toSeq.filter {
      case (exprV3_4, _) => expressionMapping.isDefinedAt((exprV3_4, exprV3_4.position))
    }.map {
      case (exprV3_4, typeInfoV3_4) => (expressionMapping((exprV3_4, exprV3_4.position)), convert(typeInfoV3_4))
    }
    astv4_0.ASTAnnotationMap(result:_*)
  }

  private def convert(resolvedRelTypeNames: mutable.Map[String, utilV3_4.RelTypeId]): mutable.Map[String, utilv4_0.RelTypeId] = {
    val res = new mutable.HashMap[String, utilv4_0.RelTypeId]
    resolvedRelTypeNames.foreach {
      case (key, r3_4) => res += ((key, utilv4_0.RelTypeId(r3_4.id)))
    }
    res
  }

  private def convert(typeInfoV3_4: semanticsV3_4.ExpressionTypeInfo): astv4_0.semantics.ExpressionTypeInfo = typeInfoV3_4 match {
    case semanticsV3_4.ExpressionTypeInfo(specified, expected) =>
      astv4_0.semantics.ExpressionTypeInfo(convert(specified), expected.map(convert))
  }

  private def convert(specified: utilV3_4.symbols.TypeSpec): utilv4_0.symbols.TypeSpec = {
      new utilv4_0.symbols.TypeSpec(specified.ranges.map(convert))
  }

  private def convert(range: utilV3_4.symbols.TypeRange): utilv4_0.symbols.TypeRange = range match {
    case utilV3_4.symbols.TypeRange(lower, upper) =>
      utilv4_0.symbols.TypeRange(convert(lower), upper.map(convert))
  }

  private def convert(cypherType: utilV3_4.symbols.CypherType): utilv4_0.symbols.CypherType = cypherType match {
    case utilV3_4.symbols.CTAny => utilv4_0.symbols.CTAny
    case utilV3_4.symbols.CTBoolean => utilv4_0.symbols.CTBoolean
    case utilV3_4.symbols.CTFloat => utilv4_0.symbols.CTFloat
    case utilV3_4.symbols.CTGeometry => utilv4_0.symbols.CTGeometry
    case utilV3_4.symbols.CTGraphRef => utilv4_0.symbols.CTGraphRef
    case utilV3_4.symbols.CTInteger => utilv4_0.symbols.CTInteger
    case utilV3_4.symbols.ListType(iteratedType) => utilv4_0.symbols.CTList(convert(iteratedType))
    case utilV3_4.symbols.CTMap => utilv4_0.symbols.CTMap
    case utilV3_4.symbols.CTNode => utilv4_0.symbols.CTNode
    case utilV3_4.symbols.CTNumber => utilv4_0.symbols.CTNumber
    case utilV3_4.symbols.CTPath => utilv4_0.symbols.CTPath
    case utilV3_4.symbols.CTPoint => utilv4_0.symbols.CTPoint
    case utilV3_4.symbols.CTRelationship => utilv4_0.symbols.CTRelationship
    case utilV3_4.symbols.CTString => utilv4_0.symbols.CTString
    case utilV3_4.symbols.CTDate => utilv4_0.symbols.CTDate
    case utilV3_4.symbols.CTDateTime => utilv4_0.symbols.CTDateTime
    case utilV3_4.symbols.CTTime => utilv4_0.symbols.CTTime
    case utilV3_4.symbols.CTLocalTime => utilv4_0.symbols.CTLocalTime
    case utilV3_4.symbols.CTLocalDateTime => utilv4_0.symbols.CTLocalDateTime
    case utilV3_4.symbols.CTDuration => utilv4_0.symbols.CTDuration
  }
}
