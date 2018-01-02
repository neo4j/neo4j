/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.symbols._

object LiteralTypeSupport {
  def deriveType(obj: Any): CypherType = obj match {
    case _: String                          => CTString
    case _: Char                            => CTString
    case _: Integer|_:Long|_:Short|_:Byte   => CTInteger
    case _: Number                          => CTFloat
    case _: Boolean                         => CTBoolean
    case IsMap(_)                           => CTMap
    case IsCollection(coll) if coll.isEmpty => CTCollection(CTAny)
    case IsCollection(coll)                 => CTCollection(coll.map(deriveType).reduce(_ leastUpperBound _))
    case _                                  => CTAny
  }
}
