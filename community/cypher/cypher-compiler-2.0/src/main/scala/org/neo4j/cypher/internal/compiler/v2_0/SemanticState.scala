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
package org.neo4j.cypher.internal.compiler.v2_0

import scala.collection.immutable.Map
import scala.collection.immutable.HashMap
import scala.collection.immutable.SortedSet
import scala.collection.breakOut
import org.neo4j.cypher.internal.compiler.v2_0.symbols._

case class Symbol(identifiers: Set[ast.Identifier], types: TypeSpec) {
  def tokens = identifiers.map(_.token)(breakOut[Set[ast.Identifier], InputToken, SortedSet[InputToken]])
}

case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectWithCoercion)
  lazy val wasCoerced = actualUnCoerced != actual

  def expect(types: TypeSpec) = copy(expected = Some(types))
}

object SemanticState {
  val clean = SemanticState(HashMap.empty, HashMap.empty, None)
}

case class SemanticState(
    symbolTable: Map[String, Symbol],
    typeTable: Map[ast.Expression, ExpressionTypeInfo],
    parent: Option[SemanticState]) {

  def newScope = copy(symbolTable = HashMap.empty, parent = Some(this))
  def popScope = copy(symbolTable = parent.get.symbolTable, parent = parent.get.parent)

  def clearSymbols = copy(symbolTable = HashMap.empty, parent = None)

  def symbol(name: String): Option[Symbol] = symbolTable.get(name) orElse parent.flatMap(_.symbol(name))
  def symbolTypes(name: String) = this.symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importSymbols(symbols: Map[String, Symbol]) =
    copy(symbolTable = symbolTable ++ symbols)

  def declareIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    symbolTable.get(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier)))
      case Some(symbol) =>
        if (symbol.identifiers.contains(identifier))
          Right(this)
        else
          Left(SemanticError(s"${identifier.name} already declared", identifier.token, symbol.tokens))
    }

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.identifiers + identifier))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${identifier.name} already defined with conflicting type $existingTypes (expected $expectedTypes)",
            identifier.token, symbol.tokens))
        }
    }

  def ensureIdentifierDefined(identifier: ast.Identifier): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Left(SemanticError(s"${identifier.name} not defined", identifier.token))
      case Some(symbol) =>
        Right(updateIdentifier(identifier, symbol.types, symbol.identifiers + identifier))
    }

  def specifyType(expression: ast.Expression, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    expression match {
      case identifier: ast.Identifier =>
        implicitIdentifier(identifier, possibleTypes)
      case _                          =>
        Right(copy(typeTable = typeTable.updated(expression, ExpressionTypeInfo(possibleTypes))))
    }

  def expectType(expression: ast.Expression, possibleTypes: TypeSpec): (SemanticState, TypeSpec) = {
    val expType = expressionType(expression)
    val updated = expType.expect(possibleTypes)
    (copy(typeTable = typeTable.updated(expression, updated)), updated.actual)
  }

  def expressionType(expression: ast.Expression): ExpressionTypeInfo = typeTable.getOrElse(expression, ExpressionTypeInfo(TypeSpec.all))

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSpec, identifiers: Set[ast.Identifier]) =
    copy(symbolTable = symbolTable.updated(identifier.name, Symbol(identifiers, types)), typeTable = typeTable.updated(identifier, ExpressionTypeInfo(types)))
}
