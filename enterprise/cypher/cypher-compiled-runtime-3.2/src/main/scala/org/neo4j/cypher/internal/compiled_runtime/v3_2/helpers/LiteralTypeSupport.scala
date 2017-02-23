/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2.helpers

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{IsList, IsMap}
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

object LiteralTypeSupport {
  def deriveCypherType(obj: Any): CypherType = obj match {
    case _: String                          => CTString
    case _: Char                            => CTString
    case _: Integer|_:java.lang.Long|_:Int|_:Long|_:Short|_:Byte => CTInteger
    case _: Number                          => CTFloat
    case _: Boolean                         => CTBoolean
    case IsMap(_)                           => CTMap
    case IsList(coll) if coll.isEmpty       => CTList(CTAny)
    case IsList(coll)                       => CTList(coll.map(deriveCypherType).reduce(_ leastUpperBound _))
    case _                                  => CTAny
  }

  def deriveCodeGenType(obj: Any): CypherCodeGenType = deriveCodeGenType(deriveCypherType(obj))

  def deriveCodeGenType(ct: CypherType): CypherCodeGenType = ct match {
    case ListType(innerCt) => CypherCodeGenType(CTList(innerCt), ListReferenceType(toRepresentationType(innerCt)))
    case _ => CypherCodeGenType(ct, toRepresentationType(ct))
  }

  private def toRepresentationType(ct: CypherType): RepresentationType = ct match {
    case CTInteger => LongType
    case CTFloat => expressions.FloatType
    case CTBoolean => BoolType
    case CTNode => LongType
    case CTRelationship => LongType
    case _ => ReferenceType
  }
}
