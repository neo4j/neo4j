package org.neo4j.cypher.pipes

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.cypher.commands.Match
import org.neo4j.cypher.PatternContext
import org.neo4j.cypher.SymbolTable
import collection.immutable.Map

class PatternPipe(source: Pipe, matching: Option[Match]) extends Pipe {

  var patternContext: PatternContext = null

  def prepare(symbolTable: SymbolTable) {
    patternContext = new PatternContext(symbolTable)
    patternContext.createPatterns(matching)
    patternContext.checkConnectednessOfPatternGraph(source.columns)
  }

  def foreach[U](f: Map[String, Any] => U) {
    source.foreach((row) => {
      row.foreach(patternContext.bindStartPoint(_))

      patternContext.getPatternMatches(row).map(f)
    })
  }

  def columns = source.columns // TODO ++ patternContext.columns
}