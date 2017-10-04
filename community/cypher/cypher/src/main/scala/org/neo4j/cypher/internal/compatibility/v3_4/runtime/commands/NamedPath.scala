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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.AbstractPattern
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Expression

case class NamedPath(pathName: String, pathPattern: AbstractPattern*) extends Traversable[AbstractPattern]  {
  def foreach[U](f: (AbstractPattern) => U) {
    pathPattern.foreach(f)
  }

  def rewrite(f: Expression => Expression) = NamedPath(pathName, pathPattern.map(_.rewrite(f)): _*)

  override def toString() = "NamedPath(%s = %s)".format(pathName, pathPattern.mkString(","))
}

