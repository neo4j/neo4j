/*
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
package org.neo4j.cypher.internal.compiler.v3_1.codegen

import java.time.Clock

/**
  * Configuration modes for code generation
  */
sealed trait CodeGenMode

/**
  * Produces source code
  */
case object SourceCodeMode extends CodeGenMode

/**
  * Produce byte code directly
  */
case object ByteCodeMode extends CodeGenMode


/**
  * Configuration class for code generation
 *
  * @param mode The mode of code generation
  * @param saveSource if `true` source code is stored and returned
  * @param clock Clock used for keeping track of time
  * @param packageName The name of the package the produced code should belong to
  */
case class CodeGenConfiguration(mode: CodeGenMode = ByteCodeMode,
                                saveSource: Boolean = false,
                                clock: Clock = Clock.systemUTC(),
                                packageName: String = "org.neo4j.cypher.internal.compiler.v3_1.generated"
                               )
