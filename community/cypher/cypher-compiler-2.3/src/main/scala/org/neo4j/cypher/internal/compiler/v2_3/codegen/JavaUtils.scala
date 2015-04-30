/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen

import org.apache.commons.lang3.StringEscapeUtils

object JavaUtils {

  implicit class JavaString(name: String) {
    def toJava = s"""${StringEscapeUtils.escapeJava(name)}"""
  }

  object JavaTypes {
    val LONG = "long"
    val INT = "int"
    val OBJECT = "Object"
    val LIST = "java.util.List"
    val MAP = "java.util.Map"
    val DOUBLE = "double"
    val STRING = "String"
    val NUMBER = "Number"
    val NODE = "org.neo4j.graph.Node"
    val RELATIONSHIP = "org.neo4j.graph.Relationship"
  }

  case class JavaSymbol(name: String, javaType: String, materializedSymbol: Option[JavaSymbol] = None) {
    self =>

    def materialize = materializedSymbol.getOrElse(self)

    def withProjectedSymbol(symbol: JavaSymbol) = copy(materializedSymbol = Some(symbol))
  }
}
