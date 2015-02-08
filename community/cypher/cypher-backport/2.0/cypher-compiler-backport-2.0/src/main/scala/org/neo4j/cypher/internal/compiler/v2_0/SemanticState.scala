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
package org.neo4j.cypher.internal.compiler.v2_0

import symbols._
import scala.collection.immutable.HashMap

case class Symbol(locations: Seq[ast.Identifier], types: TypeSpec) {
  def positions = locations.map(_.position)
}

case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced = actualUnCoerced != actual

  def expect(types: TypeSpec) = copy(expected = Some(types))
}

object SemanticState {
  val clean = SemanticState(HashMap.empty, IdentityMap.empty, None)
}

case class SemanticState private (
    symbolTable: Map[String, Symbol],
    typeTable: IdentityMap[ast.Expression, ExpressionTypeInfo],
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
        Right(updateIdentifier(identifier, possibleTypes, Seq(identifier)))
      case Some(symbol) =>
        Left(SemanticError(s"${identifier.name} already declared", identifier.position, symbol.positions:_*))
    }

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Seq(identifier)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.locations :+ identifier))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${identifier.name} already defined with conflicting type $existingTypes (expected $expectedTypes)",
            identifier.position, symbol.positions:_*))
        }
    }

  def ensureIdentifierDefined(identifier: ast.Identifier): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Left(SemanticError(s"${identifier.name} not defined", identifier.position))
      case Some(symbol) =>
        Right(updateIdentifier(identifier, symbol.types, symbol.locations :+ identifier))
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

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSpec, locations: Seq[ast.Identifier]) =
    copy(symbolTable = symbolTable.updated(identifier.name, Symbol(locations, types)), typeTable = typeTable.updated(identifier, ExpressionTypeInfo(types)))
}
