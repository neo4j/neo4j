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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.perty.bling._
import org.neo4j.cypher.internal.compiler.v2_2.perty.ops.DocOp
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.PrintCommand

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * See pp.Doc
 */
package object perty {
  // Imperative description of how to construct a document
  // that contains leaf values of T
  type DocOps[T] = Seq[DocOp[T]]

  // convert a value into a doc (digger)
  type DocGen[-T] = SeqDrill[T, Doc]

  // convert a value into a doc (total function)
  type DocConverter[-T] = T => Doc

  // layout a doc as a series of print commands
  type DocFormatter = Doc => Seq[PrintCommand]

  // turns a sequence of print commands into a result of type T
  type PrintingConverter[+T] = mutable.Builder[PrintCommand, T]

  // drills used by DocGens
  type DocDrill[-T] = Drill[T, Doc]
}


