/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.perty.{NestedDocGenerator, Doc}
import scala.reflect.ClassTag
import scala.util.Try

case class catchErrors[T: ClassTag](instance: NestedDocGenerator[T]) extends NestedDocGenerator[T] {

  import Doc._

  override def isDefinedAt(v: T) =
    try {
      instance.isDefinedAt(v)
    }
    catch {
      case _: NotImplementedError => true
      case _: MatchError          => true
    }

  override def apply(v: T) =
    (inner) =>
      try {
        instance(v)(inner)
      }
      catch {
        case _: NotImplementedError =>
          "???"

        case e: Exception  =>
          group("!!!" :/: Try(text(e.getMessage)).getOrElse(e.getClass.toString :: ":" :/: "Failed to get the message"))
      }
}
