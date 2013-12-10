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

  def newScope = copy(symbolTable = HashMap.empty, parent = Some(this))
  def popScope = copy(symbolTable = parent.get.symbolTable, parent = parent.get.parent)

  def clearSymbols = copy(symbolTable = HashMap.empty, parent = None)

  def symbol(name: String): Option[Symbol] = symbolTable.get(name) orElse parent.flatMap(_.symbol(name))
  def symbolTypes(name: String) = this.symbol(name).map(_.types).getOrElse(TypeSet.all)

  def declareIdentifier(identifier: ast.Identifier, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    declareIdentifier(identifier, TypeSet(possibleType +: possibleTypes))

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
    implicitIdentifier(identifier, TypeSet(possibleType +: possibleTypes))

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier)))
      case Some(symbol) =>
        val inferredTypes = (symbol.types constrain possibleTypes)
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.identifiers + identifier))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
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

  def expressionTypes(expression: ast.Expression): TypeSet = typeTable.get(expression).getOrElse(TypeSet.all)

  def specifyType(expression: ast.Expression, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    specifyType(expression, TypeSet(possibleType +: possibleTypes))

  def specifyType(expression: ast.Expression, possibleTypes: TypeSet): Either[SemanticError, SemanticState] =
    expression match {
      case identifier: ast.Identifier => implicitIdentifier(identifier, possibleTypes)
      case _                          => Right(copy(typeTable = typeTable + ((expression, possibleTypes))))
    }

  def constrainType(expression: ast.Expression, possibleType: CypherType, possibleTypes: CypherType*): Either[SemanticError, SemanticState] =
    constrainType(expression, TypeSet(possibleType +: possibleTypes))

  def constrainType(expression: ast.Expression, possibleTypes: TypeSet): Either[SemanticError, SemanticState] = {
    val currentTypes = expressionTypes(expression)
    val inferredTypes = (currentTypes constrain possibleTypes)
    if (inferredTypes.nonEmpty) {
      Right(updateType(expression, inferredTypes))
    } else {
      Left(typeMismatch(expression, currentTypes, possibleTypes))
    }
  }

  def importSymbols(symbols: Map[String, Symbol]) =
    copy(symbolTable = symbolTable ++ symbols)

  private def updateType(expression: ast.Expression, types: TypeSet) = expression match {
    case identifier: ast.Identifier =>
      val identifiers = symbol(identifier.name).map(_.identifiers).getOrElse(Set.empty)
      updateIdentifier(identifier, types, identifiers + identifier)
    case _ =>
      copy(typeTable = typeTable + ((expression, types)))
  }

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSet, identifiers: Set[ast.Identifier]) =
    copy(symbolTable = symbolTable + ((identifier.name, Symbol(identifiers, types))), typeTable = typeTable + ((identifier, types)))

  private def typeMismatch(expression: ast.Expression, existingTypes: TypeSet, expectedTypes: TypeSet) = {
    val existingTypesString = existingTypes.mkString(", ", " or ")
    val expectedTypesString = expectedTypes.mkString(", ", " or ")
    expression match {
      case identifier: ast.Identifier =>
        val tokens = symbol(identifier.name).map(_.tokens).getOrElse(SortedSet.empty[InputToken]) - identifier.token
        SemanticError(s"Type mismatch: ${identifier.name} already defined with conflicting type ${existingTypesString} (expected ${expectedTypesString})", identifier.token, tokens)
      case _ =>
        SemanticError(s"Type mismatch: expected ${expectedTypesString} but was ${existingTypesString}", expression.token)
    }
  }
}
