/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty

import org.neo4j.cypher.internal.compiler.v2_2.perty.bling.SingleFunDigger
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.pprintToString

import scala.reflect.ClassTag

abstract class CustomDocGen[-T : ClassTag] extends SingleFunDigger[T, Any, Doc] {
  object drill extends DocDrill[T] {
    private val impl: DocDrill[T] = newDocDrill

    def apply(v: T) = impl(v)
  }

  protected def newDocDrill: DocDrill[T]

  trait ToString[S <: T] {
    prettySelf: S with DocFormatting =>

    override def toString =
      pprintToString[S](prettySelf, formatter = docFormatter)(asConverter)
  }
}
