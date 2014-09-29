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
package org.neo4j.cypher.internal.compiler.v2_2.perty.gen

import scala.reflect.runtime.universe._

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.{pprintToDoc, PrintCommand}

abstract class DocHandlerTestSuite[T]
  extends CypherFunSuite
  with DocHandler[T]
  with LineDocFormatting {

  def pprint[S <: T : TypeTag](value: S, formatter: DocFormatter = docFormatter): Unit =
    print.pprint(value, formatter)(docGen)

  def pprintToString[S <: T : TypeTag](value: S, formatter: DocFormatter = docFormatter): String =
    print.pprintToString(value, formatter)(docGen)

  def convert[S <: T : TypeTag](value: S, formatter: DocFormatter = docFormatter): Doc =
    pprintToDoc(value)(docGen)

  def format[S <: T : TypeTag](value: S, formatter: DocFormatter = docFormatter): Seq[PrintCommand] =
    formatter(convert(value))
}
