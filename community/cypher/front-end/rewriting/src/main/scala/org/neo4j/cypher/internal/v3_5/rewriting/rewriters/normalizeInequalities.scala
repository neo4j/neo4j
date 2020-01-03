/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.rewriting.rewriters

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, topDown}

case object normalizeInequalities extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case o@Or(Equals(a1, b1), LessThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o@Or(Equals(b1, a1), LessThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o@Or(LessThan(a2, b2), Equals(a1, b1)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o@Or(LessThan(a2, b2), Equals(b1, a1)) if a1 == a2 && b1 == b2 =>
      LessThanOrEqual(a1, b1)(o.position)
    case o@Or(Equals(a1, b1), GreaterThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o@Or(Equals(b1, a1), GreaterThan(a2, b2)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o@Or(GreaterThan(a2, b2), Equals(a1, b1)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
    case o@Or(GreaterThan(a2, b2), Equals(b1, a1)) if a1 == a2 && b1 == b2 =>
      GreaterThanOrEqual(a1, b1)(o.position)
  })
}
