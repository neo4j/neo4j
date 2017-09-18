/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.symbols

import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{CypherException, CypherTypeException, SyntaxException}

import scala.collection.Map

case class SymbolTable(variables: Map[String, CypherType] = Map.empty) {
  def hasVariableNamed(name: String): Boolean = variables.contains(name)
  def size: Int = variables.size
  def isEmpty: Boolean = variables.isEmpty

  def add(key: String, typ: CypherType): SymbolTable = SymbolTable(variables + (key -> typ))

  def add(value: Map[String, CypherType]): SymbolTable = {
    value.foldLeft(this) {
      (a: SymbolTable, b: (String, CypherType)) => a.add(b._1, b._2)
    }
  }

  def filter(f: String => Boolean): SymbolTable = SymbolTable(variables.filterKeys(f))
  def keys: Seq[String] = variables.keys.toIndexedSeq
  def missingSymbolTableDependencies(x: TypeSafe): Set[String] = x.symbolTableDependencies.filterNot(dep => variables.exists(_._1 == dep))

  def evaluateType(name: String, expectedType: CypherType): CypherType = variables.get(name) match {
    case Some(typ) if expectedType.isAssignableFrom(typ) => typ
    case Some(typ) if typ.isAssignableFrom(expectedType) => typ
    case Some(typ)                                       => throw new CypherTypeException("Expected `%s` to be a %s but it was a %s".format(name, expectedType, typ))
    case None                                            => throw new SyntaxException("Unknown variable `%s`.".format(name))
  }

  def checkType(name: String, expectedType: CypherType): Boolean = try {
    evaluateType(name, expectedType)
    true
  } catch {
    case _: CypherException => false
  }

  def intersect(other: SymbolTable): SymbolTable = {
    val overlap = variables.keySet intersect other.variables.keySet

    val mergedVariables = (overlap map {
      key => key -> variables(key).leastUpperBound(other.variables(key))
    }).toMap

    SymbolTable(mergedVariables)
  }
}

/*
TypeSafe is everything that needs to check it's types
 */
trait TypeSafe {
  def symbolTableDependencies: Set[String]
}
