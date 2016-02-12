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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

case class coerceToTypeIfNeeded(types: ASTAnnotationMap[Expression, ExpressionTypeInfo])
  extends ((Expression, CypherType) => Expression) {

  def apply(expr: Expression, expected: CypherType) = {
    val typeInfo = types(expr)
    val actual = typeInfo.actual

    val exprIsAssignableToExpectedType = actual.iterator.forall(expected.isAssignableFrom)
    if (exprIsAssignableToExpectedType) {
      expr
    } else {
      val leastCommonExpectedSuperType =
        expected
          .iterator
          .reduceLeftOption { (t1, t2) => t1.leastUpperBound(t2) }
      val coercionTargetType = leastCommonExpectedSuperType.filter(_ != CTAny)
      coercionTargetType.map { cypherType => CoerceTo(expr, cypherType) }.getOrElse(expr)
    }
  }
}
