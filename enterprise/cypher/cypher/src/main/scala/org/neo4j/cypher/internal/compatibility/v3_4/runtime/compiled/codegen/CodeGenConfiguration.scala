/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen

import java.nio.file.{Path, Paths}

import org.neo4j.cypher.internal.util.v3_4.InternalException

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
  * @param showSource if `true` source code is stored and returned
  * @param packageName The name of the v3_4 the produced code should belong to
  */
case class CodeGenConfiguration(mode: CodeGenMode = CodeGenMode.default,
                                showSource: Boolean = false,
                                showByteCode: Boolean = false,
                                saveSource: Option[Path] = None,
                                packageName: String = "org.neo4j.cypher.internal.compiler.v3_4.generated"
                               )

object CodeGenConfiguration {
  def apply(debugOptions: Set[String]): CodeGenConfiguration = {
    val mode = if(debugOptions.contains("generate_java_source")) SourceCodeMode else ByteCodeMode
    val show_java_source = debugOptions.contains("show_java_source")
    if (show_java_source && mode != SourceCodeMode) {
      throw new InternalException("Can only 'debug=show_java_source' if 'debug=generate_java_source'.")
    }
    val show_bytecode = debugOptions.contains("show_bytecode")
    val saveSource = Option(System.getProperty("org.neo4j.cypher.DEBUG.generated_source_location")).map(Paths.get(_))
    CodeGenConfiguration(mode, show_java_source, show_bytecode, saveSource)
  }
}

object CodeGenMode {
  val default = ByteCodeMode
}
