package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.{SemanticTable => SemanticTableV3_3, ast => astV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.{ast => astV3_4}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{ExpressionTypeInfo, SemanticTable => SemanticTableV3_4}
import org.neo4j.cypher.internal.frontend.{v3_3 => frontendV3_3, v3_4 => frontendV3_4}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsV3_4}
import org.neo4j.cypher.internal.util.{v3_4 => utilV3_4}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}

import scala.collection.mutable

object SemanticTableConverter {

  // TODO key (Expr, InputPosition) instead of just Expr ? in expressionMapping

  // The semantic table needs to have the same objects as keys, therefore we cannot convert expressions again
  // but must use the already converted 3.4 instances
  def convertSemanticTable(table: SemanticTableV3_3, expressionMapping: Map[ExpressionV3_3, ExpressionV3_4]): SemanticTableV3_4 = {
    new SemanticTableV3_4(
      convert(table.types, expressionMapping),
      astV3_4.ASTAnnotationMap.empty,
      new mutable.HashMap[String, utilV3_4.LabelId],
      new mutable.HashMap[String, utilV3_4.PropertyKeyId],
      convert(table.resolvedRelTypeNames))
  }


  private def convert(types: astV3_3.ASTAnnotationMap[astV3_3.Expression, frontendV3_3.ExpressionTypeInfo], expressionMapping: Map[ExpressionV3_3, ExpressionV3_4]):
  astV3_4.ASTAnnotationMap[expressionsV3_4.Expression, frontendV3_4.semantics.ExpressionTypeInfo] = {
    val x: Map[ExpressionV3_4, ExpressionTypeInfo] = types.map {
      case (exprV3_3, typeInfoV3_3) => (expressionMapping(exprV3_3), convert(typeInfoV3_3))
    }
    astV3_4.ASTAnnotationMap(x.toSeq:_*)
  }

  private def convert(resolvedRelTypeNames: mutable.Map[String, frontendV3_3.RelTypeId]): mutable.Map[String, utilV3_4.RelTypeId] = {
    val res = new mutable.HashMap[String, utilV3_4.RelTypeId]
    resolvedRelTypeNames.foreach {
      case (key, r3_3) => res += ((key, utilV3_4.RelTypeId(r3_3.id)))
    }
    res
  }


  private def convert(typeInfoV3_3: frontendV3_3.ExpressionTypeInfo): frontendV3_4.semantics.ExpressionTypeInfo = typeInfoV3_3 match {
    case frontendV3_3.ExpressionTypeInfo(specified, expected) =>
      frontendV3_4.semantics.ExpressionTypeInfo(convert(specified), expected.map(convert _))
  }

  private def convert(specified: frontendV3_3.symbols.TypeSpec): utilV3_4.symbols.TypeSpec = specified match {
    case t: frontendV3_3.symbols.TypeSpec =>
      new utilV3_4.symbols.TypeSpec(???) // TODO use specified.ranges.map(convert _) as soon as 3.3.1 is in
  }

  private def convert(range: frontendV3_3.symbols.TypeRange): utilV3_4.symbols.TypeRange = range match {
    case frontendV3_3.symbols.TypeRange(lower, upper) =>
      utilV3_4.symbols.TypeRange(convert(lower), upper.map(convert _))
  }

  private def convert(cypherType: frontendV3_3.symbols.CypherType): utilV3_4.symbols.CypherType = cypherType match {
    case frontendV3_3.symbols.CTAny => utilV3_4.symbols.CTAny
    case frontendV3_3.symbols.CTBoolean => utilV3_4.symbols.CTBoolean
    case frontendV3_3.symbols.CTFloat => utilV3_4.symbols.CTFloat
    case frontendV3_3.symbols.CTGeometry => utilV3_4.symbols.CTGeometry
    case frontendV3_3.symbols.CTGraphRef => utilV3_4.symbols.CTGraphRef
    case frontendV3_3.symbols.CTInteger => utilV3_4.symbols.CTInteger
    case frontendV3_3.symbols.ListType(iteratedType) => utilV3_4.symbols.CTList(convert(iteratedType))
    case frontendV3_3.symbols.CTMap => utilV3_4.symbols.CTMap
    case frontendV3_3.symbols.CTNode => utilV3_4.symbols.CTNode
    case frontendV3_3.symbols.CTNumber => utilV3_4.symbols.CTNumber
    case frontendV3_3.symbols.CTPath => utilV3_4.symbols.CTPath
    case frontendV3_3.symbols.CTPoint => utilV3_4.symbols.CTPoint
    case frontendV3_3.symbols.CTRelationship => utilV3_4.symbols.CTRelationship
    case frontendV3_3.symbols.CTString => utilV3_4.symbols.CTString
  }
}
