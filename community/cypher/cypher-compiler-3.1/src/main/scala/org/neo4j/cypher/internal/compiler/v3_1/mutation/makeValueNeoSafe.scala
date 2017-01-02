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
package org.neo4j.cypher.internal.compiler.v3_1.mutation

import org.neo4j.cypher.internal.compiler.v3_1.helpers.{CastSupport, ListSupport}

object makeValueNeoSafe extends (Any => Any) with ListSupport {

  def apply(a: Any): Any = if (isList(a)) {
    transformTraversableToArray(makeTraversable(a))
  } else {
    a
  }

  /*
  This method finds the type that we can use for the primitive array that Neo4j wants
  We can't just find the nearest common supertype - we need a type that the other values
  can be coerced to according to Cypher coercion rules
   */
  private def transformTraversableToArray(a: Any): Any = {
    val seq: Seq[Any] = a.asInstanceOf[Traversable[_]].toIndexedSeq

    if (seq.isEmpty) {
      Array[String]()
    } else {
      val typeValue = seq.reduce(CastSupport.merge)
      val converter = CastSupport.getConverter(typeValue)

      converter.arrayConverter(seq.map(converter.valueConverter))
    }
  }

}
