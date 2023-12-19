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
import org.neo4j.cypher.internal.util.v3_4.symbols._

case class Addition(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression with BinaryOperator {

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = structure.addExpression

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def name: String = "add"

  override def codeGenType(implicit context: CodeGenContext) = (lhs.codeGenType.ct, rhs.codeGenType.ct) match {

    // Collections
    case (ListType(left), ListType(right)) =>
      CypherCodeGenType(ListType(left leastUpperBound right), ReferenceType)
    case (ListType(innerType), singleElement) =>
      CypherCodeGenType(ListType(innerType leastUpperBound singleElement), ReferenceType)
    case (singleElement, ListType(innerType)) =>
      CypherCodeGenType(ListType(innerType leastUpperBound singleElement), ReferenceType)

    case (CTAny, _) => CypherCodeGenType(CTAny, ReferenceType)
    case (_, CTAny) => CypherCodeGenType(CTAny, ReferenceType)

    // Strings
    case (CTString, _) => CypherCodeGenType(CTString, ReferenceType)
    case (_, CTString) => CypherCodeGenType(CTString, ReferenceType)

    // Numbers
    case (CTInteger, CTInteger) => CypherCodeGenType(CTInteger, ReferenceType)
    case (Number(_), Number(_)) => CypherCodeGenType(CTFloat, ReferenceType)

    // Runtime we'll figure it out
    case _ => CodeGenType.Any
  }
}
