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

import org.neo4j.cypher.internal.compiler.v2_2.perty.ops.{BaseDocOp, DocOp}
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.PrintCommand

import scala.collection.mutable

/**
 * See pp.Doc
 */
package object perty {
  // description of how to construct a doc
  // that still may contain content that needs further processing
  //
  // You should build these using DocGen and NewPretty
  //
  type DocOps[+T] = Seq[DocOp[T]]
  type DocGen[-T] = Extractor[T, DocOps[Any]]

  // description of how to construct a doc (you won't work with these
  // types usually)
  type BaseDocOps = Seq[BaseDocOp]
  type BaseDocGen[-T] = Extractor[T, BaseDocOps]

  // layout a doc as a series of print commands
  type DocFormatter = Doc => Seq[PrintCommand]

  // turns a sequence of print commands into a result of type T
  type PrintingConverter[+T] = mutable.Builder[PrintCommand, T]
}


