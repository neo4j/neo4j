/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression

import scala.collection.Map

trait GraphElementPropertyFunctions extends ListSupport {

  implicit class RichMap(m: Map[String, Expression]) {

    def rewrite(f: (Expression) => Expression): Map[String, Expression] = m.map {
      case (k, v) => k -> v.rewrite(f)
    }

    def symboltableDependencies: Set[String] = m.values.flatMap(_.symbolTableDependencies).toSet
  }

  def toString(m: Map[String, Expression]): String = m.map {
    case (k, e) => "%s: %s".format(k, e.toString)
  }.mkString("{", ", ", "}")
}


