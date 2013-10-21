/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import scala.collection.immutable.Map
import scala.collection.immutable.HashMap
import scala.collection.immutable.SortedSet
import scala.collection.breakOut
import org.neo4j.cypher.internal.compiler.v2_0.symbols._

case class Symbol(identifiers: Set[ast.Identifier], types: TypeSet) {
  def tokens = identifiers.map(_.token)(breakOut[Set[ast.Identifier], InputToken, SortedSet[InputToken]])
}

object SemanticState {
  val clean = SemanticState(HashMap.empty, HashMap.empty, None)
}

case class SemanticState(
    symbolTable: Map[String, Symbol],
    typeTable: Map[ast.Expression, TypeSet],
    parent: Option[SemanticState]) {

  def newScope = SemanticState(HashMap.empty, typeTable, Some(this))
  def popScope = SemanticState(parent.get.symbolTable, typeTable, parent.get.parent)

  def clearSymbols = SemanticState(HashMap.empty, typeTable, None)

  def symbol(name: String): Option[Symbol] = symbolTable.get(name) orElse parent.flatMap(_.symbol(name))
  def symbolTypes(name: String) = this.symbol(name).map(_.types).getOrElse(TypeSet.empty)

  def expressionTypes(expression: ast.Expression): TypeSet = typeTable.get(expression).getOrElse(TypeSet.empty)

  def specifyType(expression: ast.Expression, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    specifyType(expression, (possibleType +: possibleTypes).toSet)

  def specifyType(expression: ast.Expression, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    expression match {
      case identifier: ast.Identifier => implicitIdentifier(identifier, possibleTypes)
      case _                          => Right(SemanticState(symbolTable, typeTable + ((expression, possibleTypes)), parent))
    }

  def constrainType(expression: ast.Expression, token: InputToken, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    constrainType(expression, token, (possibleType +: possibleTypes).toSet)

  def constrainType(expression: ast.Expression, token: InputToken, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    expression match {
      case identifier: ast.Identifier => implicitIdentifier(identifier, possibleTypes)
      case _                          =>
        val currentTypes = expressionTypes(expression)
        val inferredTypes = (currentTypes mergeUp possibleTypes)
        if (inferredTypes.nonEmpty) {
          Right(updateType(expression, inferredTypes))
        } else {
          val existingTypes = currentTypes.formattedString
          val expectedTypes = possibleTypes.formattedString
          Left(SemanticError(s"Type mismatch: expected ${expectedTypes} but was ${existingTypes}", token, expression.token))
        }
    }

  def declareIdentifier(identifier: ast.Identifier, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    declareIdentifier(identifier, (possibleType +: possibleTypes).toSet)

  def declareIdentifier(identifier: ast.Identifier, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    symbolTable.get(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier)))
      case Some(symbol) =>
        if (symbol.identifiers.contains(identifier))
          Right(this)
        else
          Left(SemanticError(s"${identifier.name} already declared", identifier.token, symbol.tokens))
    }

  def implicitIdentifier(identifier: ast.Identifier, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    implicitIdentifier(identifier, (possibleType +: possibleTypes).toSet)

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier)))
      case Some(symbol) =>
        val inferredTypes = (symbol.types mergeUp possibleTypes)
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.identifiers + identifier))
        } else {
          val existingTypes = symbol.types.formattedString
          val expectedTypes = possibleTypes.formattedString
          Left(SemanticError(
            s"Type mismatch: ${identifier.name} already defined with conflicting type ${existingTypes} (expected ${expectedTypes})",
            identifier.token, symbol.tokens))
        }
    }

  def ensureIdentifierDefined(identifier: ast.Identifier): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         => Left(SemanticError(s"${identifier.name} not defined", identifier.token))
      case Some(symbol) => Right(updateIdentifier(identifier, symbol.types, symbol.identifiers + identifier))
    }

  def importSymbols(symbols: Map[String, Symbol]) =
    SemanticState(symbolTable ++ symbols, typeTable, parent)

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSet, identifiers: Set[ast.Identifier]) =
    SemanticState(symbolTable + ((identifier.name, Symbol(identifiers, types))), typeTable + ((identifier, types)), parent)

  private def updateType(expression: ast.Expression, types: TypeSet) =
    SemanticState(symbolTable, typeTable + ((expression, types)), parent)
}
