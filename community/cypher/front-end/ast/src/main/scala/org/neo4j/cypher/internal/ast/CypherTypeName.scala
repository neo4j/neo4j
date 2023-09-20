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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.NothingType

/**
 * Please note when introducing new types here to make sure the accompanying valueType() function is considered.
 * If a more specific type is added, the valueType() function should *not* return it until the next major release as
 * this is considered a breaking change. Completely new types are okay.
 * E.g currently will return "FLOAT", if FLOAT32 is introduced it should continue to return "FLOAT" until next major release.
 */
case class CypherTypeName(cypherType: CypherType) extends SemanticCheckable with SemanticAnalysisTooling {

  override def semanticCheck: SemanticCheck = {
    cypherTypeSemanticCheck(cypherType)
  }

  private def cypherTypeSemanticCheck(cypherType: CypherType): SemanticCheck = {
    cypherType match {
      case ListType(innerType: ClosedDynamicUnionType, _) => cypherTypeSemanticCheck(innerType)
      case unionInnerType: ClosedDynamicUnionType         =>
        // All types are nullable or all are not nullable
        // The following throw semantic errors:
        //    * ANY<INTEGER | FLOAT NOT NULL>
        //    * ANY<INTEGER NOT NULL | FLOAT>
        // NOTE: NOTHING is always NOT NULL, but should work e.g ANY<NOTHING | BOOLEAN> is valid
        if (
          !(unionInnerType.sortedInnerTypes.forall(innerType =>
            innerType.isNullable || innerType.isInstanceOf[NothingType]
          ) ||
            unionInnerType.sortedInnerTypes.forall(!_.isNullable))
        )
          error(
            "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
            unionInnerType.position
          )
        else semanticCheckFold(unionInnerType.sortedInnerTypes)(cypherTypeSemanticCheck)
      case _ => SemanticCheck.success
    }
  }
}
