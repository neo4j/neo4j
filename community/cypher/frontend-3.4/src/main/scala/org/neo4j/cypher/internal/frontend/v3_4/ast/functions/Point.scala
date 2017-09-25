/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.functions

import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Function, SimpleTypedFunction, _}
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, SemanticCheckResult, SemanticError}

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
