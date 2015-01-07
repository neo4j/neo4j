/**
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
package org.neo4j.cypher.internal.compiler.v1_9.symbols

import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.helpers.IsCollection
import org.neo4j.cypher.internal.compiler.v1_9.helpers.IsMap


trait CypherType {
  def isAssignableFrom(other: CypherType): Boolean = this.getClass.isAssignableFrom(other.getClass)

  def iteratedType: CypherType = throw new CypherTypeException("This is not a collection type")

  def mergeWith(other: CypherType): CypherType =
    if (this.isAssignableFrom(other)) this
    else if (other.isAssignableFrom(this)) other
    else parentType mergeWith other.parentType

  def parentType: CypherType

  val isCollection: Boolean = false
}

object CypherType {
  def fromJava(obj: Any): CypherType =
    obj match {
      case _: String          => StringType()
      case _: Number          => NumberType()
      case _: Boolean         => BooleanType()
      case IsMap(_)           => MapType()
      case IsCollection(coll) => new CollectionType(coll.map(fromJava).reduce(_ mergeWith _))
      case _                  => AnyType()
    }
}

/*
TypeSafe is everything that needs to check it's types
 */
trait TypeSafe {
  def throwIfSymbolsMissing(symbols: SymbolTable)

  def symbolDependenciesMet(symbols: SymbolTable): Boolean =
    symbolTableDependencies.forall(name => check(symbols, name))

  def symbolTableDependencies: Set[String]

  private def check(symbols: SymbolTable, name: String): Boolean = symbols.identifiers.contains(name)
}

/*
Typed is the trait all classes that have a return type, or have dependencies on an expressions' type.
 */
trait Typed {
  /*
  Checks if internal type dependencies are met, checks if the expected type is valid,
  and returns the actual type of the expression. Will throw an exception if the check fails
   */
  def evaluateType(expectedType: CypherType, symbols: SymbolTable): CypherType

  /*
  Checks if internal type dependencies are met and returns the actual type of the expression
  */
  def getType(symbols: SymbolTable): CypherType = evaluateType(AnyType(), symbols)
}
