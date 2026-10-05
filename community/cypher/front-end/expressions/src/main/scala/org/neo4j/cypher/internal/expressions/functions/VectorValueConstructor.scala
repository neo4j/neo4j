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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType

// This is just a dummy function so that the signature turns up in SHOW FUNCTIONS
// In reality, the parser will pick it up and a VectorValueConstructor Expression is created.
case object VectorValueConstructor extends Function {
  override def name = "vector"

  // TODO: Update the signature when the float16_coordinate_types_enabled flag is enabled
  override val signatures: Vector[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      this,
      names = Vector("vectorValue", "dimension", "coordinateType"),
      argumentTypes =
        Vector(ClosedDynamicUnionType(Set(CTList(CTNumber), CTString))(InputPosition.NONE), CTInteger, CTString),
      outputType = CTVector,
      description =
        "Constructs a `VECTOR` value.",
      overrideDefaultAsString = Some(
        name + "(vectorValue :: STRING | LIST<INTEGER | FLOAT>, dimension :: INTEGER, coordinateType :: [INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32]) :: VECTOR"
      ),
      overriddenArgumentTypeName =
        Some(Map("coordinateType" -> "[INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64, FLOAT32]")),
      category = Category.SCALAR,
      argumentDescriptions = Map(
        "vectorValue" -> "The numeric values to create the vector coordinates from.",
        "dimension" -> "The dimension (number of coordinates) of the vector.",
        "coordinateType" -> "The type of each coordinate in the vector."
      ),
      scopes = Set(CypherVersion.Cypher25)
    )
  )
}
