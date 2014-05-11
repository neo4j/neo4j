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
package org.neo4j.cypher.internal.compiler.v2_1.pp.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pp.{Doc, DocGenerator}

object anyDocGen extends anyDocGen

class anyDocGen extends DocGenerator[Any] {

  import Doc._

  final def apply(v: Any): Doc =
    if (null == v)
      text("null")
    else
      applyIfNotNull(v: Any): Doc

  def applyIfNotNull(v: Any): Doc =
    v match {
      case l: List[_]    => listDocGen(this)(l)
      case m: Map[_, _]  => mapDocGen(this)(m)
      case set: Set[_]   => setDocGen(this)(set)
      case seq: Seq[_]   => seqDocGen(this)(seq)
      case arr: Array[_] => arrayDocGen(this)(arr)
      case p: Product    => productDocGen(this)(p)
      case any           => valueDocGen(any)
    }
}
