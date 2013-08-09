/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.CypherCompiler


sealed trait CypherVersion {
  def name: String
  def compiler: CypherCompiler
}

object CypherVersion {
  case object v1_9 extends CypherVersion {
    val name = "1.9"
    val compiler = new internal.compiler.v1_9.CypherCompilerImpl
  }
  case object v2_0 extends CypherVersion {
    val name = "2.0"
    val compiler = new internal.compiler.v2_0.CypherCompilerImpl
  }
  case object vLegacy extends CypherVersion {
    val name = "legacy"
    val compiler = new internal.compiler.legacy.CypherCompilerImpl
  }
  val vDefault = v2_0
}
