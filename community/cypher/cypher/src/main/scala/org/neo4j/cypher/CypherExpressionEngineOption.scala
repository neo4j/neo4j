/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher

sealed abstract class CypherExpressionEngineOption(engineName: String) extends CypherOption(engineName)

case object CypherExpressionEngineOption extends CypherOptionCompanion[CypherExpressionEngineOption] {
  case object default extends CypherExpressionEngineOption("default")
  case object interpreted extends CypherExpressionEngineOption("interpreted")
  case object compiled extends CypherExpressionEngineOption("compiled")
  case object onlyWhenHot extends CypherExpressionEngineOption("only_when_hot")
  val all: Set[CypherExpressionEngineOption] = Set(interpreted, compiled, onlyWhenHot)
}