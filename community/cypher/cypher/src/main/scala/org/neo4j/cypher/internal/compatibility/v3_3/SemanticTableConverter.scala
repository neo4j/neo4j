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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.{SemanticTable => SemanticTableV3_3, ast => astV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3}
import org.opencypher.v9_0.{ast => astv3_5}
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo, SemanticTable => SemanticTablev3_5}
import org.neo4j.cypher.internal.frontend.{v3_3 => frontendV3_3}
import org.opencypher.v9_0.{expressions => expressionsv3_5}
import org.opencypher.v9_0.{util => utilv3_5}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.opencypher.v9_0.expressions.{Expression => Expressionv3_5}

import scala.collection.mutable

object SemanticTableConverter {

  type ExpressionMapping3To4 = Map[(ExpressionV3_3, InputPositionV3_3), Expressionv3_5]

  // The semantic table needs to have the same objects as keys, therefore we cannot convert expressions again
  // but must use the already converted 3.4 instances
  def convertSemanticTable(table: SemanticTableV3_3, expressionMapping: ExpressionMapping3To4): SemanticTablev3_5 = {
    new SemanticTablev3_5(
      convert(table.types, expressionMapping),
      astv3_5.ASTAnnotationMap.empty,
      new mutable.HashMap[String, utilv3_5.LabelId],
      new mutable.HashMap[String, utilv3_5.PropertyKeyId],
      convert(table.resolvedRelTypeNames))
  }

  private def convert(types: astV3_3.ASTAnnotationMap[astV3_3.Expression, frontendV3_3.ExpressionTypeInfo], expressionMapping: ExpressionMapping3To4):
  astv3_5.ASTAnnotationMap[expressionsv3_5.Expression, astv3_5.semantics.ExpressionTypeInfo] = {
    val result: Map[Expressionv3_5, ExpressionTypeInfo] = types.filter {
      case (exprV3_3, _) => expressionMapping.isDefinedAt((exprV3_3, exprV3_3.position))
    }.map {
      case (exprV3_3, typeInfoV3_3) => (expressionMapping((exprV3_3, exprV3_3.position)), convert(typeInfoV3_3))
    }
    astv3_5.ASTAnnotationMap(result.toSeq:_*)
  }

  private def convert(resolvedRelTypeNames: mutable.Map[String, frontendV3_3.RelTypeId]): mutable.Map[String, utilv3_5.RelTypeId] = {
    val res = new mutable.HashMap[String, utilv3_5.RelTypeId]
    resolvedRelTypeNames.foreach {
      case (key, r3_3) => res += ((key, utilv3_5.RelTypeId(r3_3.id)))
    }
    res
  }

  private def convert(typeInfoV3_3: frontendV3_3.ExpressionTypeInfo): astv3_5.semantics.ExpressionTypeInfo = typeInfoV3_3 match {
    case frontendV3_3.ExpressionTypeInfo(specified, expected) =>
      astv3_5.semantics.ExpressionTypeInfo(convert(specified), expected.map(convert))
  }

  private def convert(specified: frontendV3_3.symbols.TypeSpec): utilv3_5.symbols.TypeSpec = {
      new utilv3_5.symbols.TypeSpec(specified.ranges.map(convert))
  }

  private def convert(range: frontendV3_3.symbols.TypeRange): utilv3_5.symbols.TypeRange = range match {
    case frontendV3_3.symbols.TypeRange(lower, upper) =>
      utilv3_5.symbols.TypeRange(convert(lower), upper.map(convert))
  }

  private def convert(cypherType: frontendV3_3.symbols.CypherType): utilv3_5.symbols.CypherType = cypherType match {
    case frontendV3_3.symbols.CTAny => utilv3_5.symbols.CTAny
    case frontendV3_3.symbols.CTBoolean => utilv3_5.symbols.CTBoolean
    case frontendV3_3.symbols.CTFloat => utilv3_5.symbols.CTFloat
    case frontendV3_3.symbols.CTGeometry => utilv3_5.symbols.CTGeometry
    case frontendV3_3.symbols.CTGraphRef => utilv3_5.symbols.CTGraphRef
    case frontendV3_3.symbols.CTInteger => utilv3_5.symbols.CTInteger
    case frontendV3_3.symbols.ListType(iteratedType) => utilv3_5.symbols.CTList(convert(iteratedType))
    case frontendV3_3.symbols.CTMap => utilv3_5.symbols.CTMap
    case frontendV3_3.symbols.CTNode => utilv3_5.symbols.CTNode
    case frontendV3_3.symbols.CTNumber => utilv3_5.symbols.CTNumber
    case frontendV3_3.symbols.CTPath => utilv3_5.symbols.CTPath
    case frontendV3_3.symbols.CTPoint => utilv3_5.symbols.CTPoint
    case frontendV3_3.symbols.CTRelationship => utilv3_5.symbols.CTRelationship
    case frontendV3_3.symbols.CTString => utilv3_5.symbols.CTString
  }
}
