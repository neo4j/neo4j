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
package org.neo4j.cypher.internal.compiler.v2_1

import scala.collection.mutable
import org.neo4j.cypher.internal.compiler.v2_1.perty.impl.{PageDocFormatter, LineDocFormatter}
import org.neo4j.cypher.internal.helpers.PartialFunctionSupport
import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.{docStructureDocBuilder, scalaDocBuilder}

/**
 * See pp.Doc
 */
package object perty {
  type DocFormatter = Doc => Seq[PrintCommand]
  type DocGenerator[-T] = PartialFunction[T, Doc]
  type NestedDocGenerator[T] = PartialFunction[T, DocGenerator[T] => Doc]
  type PrintingConverter[+T] = mutable.Builder[PrintCommand, T]
}


