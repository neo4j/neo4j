/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.helpers.LiteralTypeSupport
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.util.v3_4.symbols.ListType

case class ListLiteral(expressions: Seq[CodeGenExpression]) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    expressions.foreach { instruction =>
      instruction.init(generator)
    }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    codeGenType match {
      case cType@CypherCodeGenType(ListType(_), ListReferenceType(innerRepr)) if RepresentationType.isPrimitive(innerRepr) =>
        structure.asPrimitiveStream(expressions.map(e => {
          e.generateExpression(structure)
        }), cType)

      case _ =>
        structure.asAnyValueList(expressions.map(e => e.codeGenType match {
          case CypherCodeGenType(ListType(_), ListReferenceType(innerRepr)) if RepresentationType.isPrimitive(innerRepr) =>
            structure.toMaterializedAnyValue(structure.iteratorFrom(e.generateExpression(structure)), e.codeGenType)
          case _ =>
            structure.toMaterializedAnyValue(e.generateExpression(structure), e.codeGenType)
        }))
    }
  }

  override def nullable(implicit context: CodeGenContext) = false

  override def codeGenType(implicit context: CodeGenContext) = {
    val commonType =
      if (expressions.nonEmpty)
        expressions.map(_.codeGenType.ct).reduce[symbols.CypherType](_ leastUpperBound _)
      else
        symbols.CTAny

    // If elements are already represented as AnyValues we may just as well keep them as such
    val representationType =
      if (expressions.nonEmpty)
        LiteralTypeSupport.selectRepresentationType(commonType, expressions.map(_.codeGenType.repr))
      else
        AnyValueType

    CypherCodeGenType(symbols.CTList(commonType), ListReferenceType(representationType))
  }
}
