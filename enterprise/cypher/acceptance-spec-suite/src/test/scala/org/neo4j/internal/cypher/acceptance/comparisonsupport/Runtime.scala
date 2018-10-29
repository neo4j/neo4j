/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Runtimes(runtimes: Runtime*)

object Runtimes {
  implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

  val all = Runtimes(CompiledBytecode, CompiledSource, Slotted, SlottedWithCompiledExpressions, Interpreted)

  def definedBy(preParserArgs: Array[String]): Runtimes = {
    val runtimes = all.runtimes.filter(_.isDefinedBy(preParserArgs))
    if (runtimes.nonEmpty) Runtimes(runtimes: _*) else all
  }

  object CompiledSource extends Runtime(Set("COMPILED", "PROCEDURE"), "runtime=compiled debug=generate_java_source")

  object CompiledBytecode extends Runtime(Set("COMPILED", "PROCEDURE"), "runtime=compiled")

  object Slotted extends Runtime(Set("SLOTTED", "PROCEDURE"), "runtime=slotted")

  object SlottedWithCompiledExpressions extends Runtime(Set("SLOTTED", "PROCEDURE"), "runtime=slotted expressionEngine=COMPILED")

  object Interpreted extends Runtime(Set("INTERPRETED", "PROCEDURE"), "runtime=interpreted")

  // Not included in `all`
  object Morsel extends Runtime(Set("MORSEL", "PROCEDURE"), "runtime=morsel")

}

case class Runtime(acceptedRuntimeNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean = preparserOption.split(" ").forall(preParserArgs.contains(_))
}
