/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1

import symbols._
import scala.collection.immutable.HashMap

case class Symbol(name: String, positions: Seq[InputPosition], types: TypeSpec)

case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced = actualUnCoerced != actual

  def expect(types: TypeSpec) = copy(expected = Some(types))
}

object SemanticState {
  val clean = SemanticState(Scope.empty, IdentityMap.empty)
}

case class Scope(symbolTable: Map[String, Symbol], parent: Option[Scope]) {
  def pushScope = copy(symbolTable = HashMap.empty, parent = Some(this))
  def popScope = parent.get

  def importScope(scope: Scope) =
    copy(symbolTable = symbolTable ++ scope.symbolTable)

  def localSymbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbol(name: String): Option[Symbol] = localSymbol(name) orElse parent.flatMap(_.symbol(name))

  def updateIdentifier(identifier: String, types: TypeSpec, positions: Seq[InputPosition]) =
    copy(symbolTable = symbolTable.updated(identifier, Symbol(identifier, positions, types)))
}

object Scope {
  val empty = Scope(symbolTable = HashMap.empty, parent = None)
}

case class SemanticState(scope: Scope, typeTable: IdentityMap[ast.Expression, ExpressionTypeInfo]) {
  def newScope = copy(scope = scope.pushScope)
  def popScope = copy(scope = scope.popScope)

  def clearSymbols = copy(scope = Scope.empty)

  def symbol(name: String): Option[Symbol] = scope.symbol(name)
  def symbolTypes(name: String) = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importScope(importedScope: Scope) = copy(scope = scope.importScope(importedScope))

  def declareIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    scope.localSymbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Seq(identifier.position)))
      case Some(symbol) =>
        Left(SemanticError(s"${identifier.name} already declared", identifier.position, symbol.positions:_*))
    }

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Seq(identifier.position)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.positions :+ identifier.position))
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
        Right(updateIdentifier(identifier, symbol.types, symbol.positions :+ identifier.position))
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

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSpec, locations: Seq[InputPosition]) =
    copy(
      scope = scope.updateIdentifier(identifier.name, types, locations),
      typeTable = typeTable.updated(identifier, ExpressionTypeInfo(types))
    )
}
