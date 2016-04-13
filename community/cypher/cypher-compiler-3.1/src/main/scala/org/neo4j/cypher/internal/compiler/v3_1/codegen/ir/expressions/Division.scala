/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{CypherTypeException, symbols}

case class Division(lhs: CodeGenExpression, rhs: CodeGenExpression)
  extends CodeGenExpression with BinaryOperator with NumericalOpType {

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    (lhs.cypherType, rhs.cypherType) match {
      case (CTBoolean, _) => throw new CypherTypeException(s"Cannot divide a boolean and ${rhs.cypherType}")
      case (_, CTBoolean) => throw new CypherTypeException(s"Cannot divide a ${rhs.cypherType} and a boolean")

      case (Number(t1), Number(t2)) => (l, r) => structure.divide(structure.box(l, t1), structure.box(r, t2))
      case (Number(t), _) => (l, r) => structure.divide(structure.box(l, t), r)
      case (_, Number(t)) => (l, r) => structure.divide(l, structure.box(r, t))

      case _ => structure.divide
    }

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def cypherType(implicit context: CodeGenContext) = symbols.CTAny
}
