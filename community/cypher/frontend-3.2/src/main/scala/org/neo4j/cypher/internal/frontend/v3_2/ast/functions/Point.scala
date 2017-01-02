/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.ast.functions

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Function, SimpleTypedFunction, _}
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticCheck, SemanticCheckResult, SemanticError}

case object Point extends Function with SimpleTypedFunction {

  override def name = "point"

  override def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation) =
    super.semanticCheck(ctx, invocation) ifOkChain checkPointMap(invocation.args(0))

  /*
   * Checks so that the point map is properly formatted
   */
  protected def checkPointMap(expression: Expression): SemanticCheck = expression match {
    //Cartesian point
    case map: MapExpression if map.items.exists(withKey("x")) && map.items.exists(withKey("y")) =>
      SemanticCheckResult.success
    //Geographic point
    case map: MapExpression if map.items.exists(withKey("longitude")) && map.items.exists(withKey("latitude")) =>
      SemanticCheckResult.success

    case map: MapExpression => SemanticError(
      s"A map with keys ${map.items.map((a) => s"'${a._1.name}'").mkString(", ")} is not describing a valid point, " +
        s"a point is described either by using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using " +
        s"geographic coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}.", map.position)

    //if using variable or parameter we can't introspect the map here
    case _ => SemanticCheckResult.success
  }

  private def withKey(key: String)(kv: (PropertyKeyName, Expression)) = kv._1.name == key

  override val signatures = Vector(
    ExpressionSignature(argumentTypes = Vector(CTMap), outputType = CTPoint)
  )
}
