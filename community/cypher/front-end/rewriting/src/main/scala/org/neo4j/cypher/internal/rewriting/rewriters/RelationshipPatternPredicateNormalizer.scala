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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.RelationshipPattern

object RelationshipPatternPredicateNormalizer extends PredicateNormalizer {

  override val extract: PartialFunction[AnyRef, IndexedSeq[Expression]] = {
    case RelationshipPattern(_, _, _, _, Some(predicate), _) => Vector(predicate)
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p @ RelationshipPattern(_, _, _, _, Some(_), _) => p.copy(predicate = None)(p.position)
  }
}
