/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.perty.gen

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintCommand, pprintToDoc}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.reflect.runtime.universe._

abstract class DocHandlerTestSuite[S : TypeTag]
  extends CypherFunSuite
  with DocHandler[S]
  with LineDocFormatting {

  def pprint[T <: S : TypeTag](value: T, formatter: DocFormatter = docFormatter): Unit =
    print.pprintln(value, formatter)(docGen)

  def pprintToString[T <: S : TypeTag](value: T, formatter: DocFormatter = docFormatter): String =
    print.pprintToString[T, S](value, formatter)(docGen)

  def convert[T <: S : TypeTag](value: T, formatter: DocFormatter = docFormatter): Doc =
    pprintToDoc[T, S](value)(docGen)

  def format[T <: S : TypeTag](value: T, formatter: DocFormatter = docFormatter): Seq[PrintCommand] =
    formatter(convert[T](value))
}
