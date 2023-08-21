/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTPoint

case object WithinBBox extends Function {
  val name = "point.withinBBox"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("point", "lowerLeft", "upperRight"),
      argumentTypes = Vector(CTPoint, CTPoint, CTPoint),
      outputType = CTBoolean,
      description = "Returns true if the provided point is within the bounding box defined by the two provided points.",
      category = Category.SPATIAL
    )
  )
}
