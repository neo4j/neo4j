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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.helpers.{TreeZipper, TreeElem}
import symbols._
import scala.collection.immutable.HashMap

// A symbol collects all uses of a postion within the current scope and
// up to the originally defining scope together with type information
//
// (s1 n1 (s2 ... (s3 n2 (s4 ... n3)) =>
//
// s1.localSymbol(n) = Symblol(n, Seq(1), type(n))
// s3.localSymbol(n) = Symblol(n, Seq(1, 2), type(n))
// s4.localSymbol(n) = Symblol(n, Seq(1, 2, 3), type(n))
//
case class Symbol(name: String, positions: Set[InputPosition], types: TypeSpec)

case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced = actualUnCoerced != actual

  def expect(types: TypeSpec) = copy(expected = Some(types))
}


object Scope {
  val empty = Scope(symbolTable = HashMap.empty, children = Vector())
}

case class Scope(symbolTable: Map[String, Symbol], children: Seq[Scope]) extends TreeElem[Scope] {
  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbolNames: Set[String] = symbolTable.keySet

  def importScope(other: Scope) =
    copy(symbolTable = symbolTable ++ other.symbolTable)

  def updateIdentifier(identifier: String, types: TypeSpec, positions: Set[InputPosition]) =
    copy(symbolTable = symbolTable.updated(identifier, Symbol(identifier, positions, types)))
}


object SemanticState {
  implicit object ScopeZipper extends TreeZipper[Scope]
  val clean = SemanticState(Scope.empty.location, IdentityMap.empty)

  implicit class ScopeLocation(val location: ScopeZipper.Location) extends AnyVal {
    def scope: Scope = location.elem
    def rootScope: Scope = location.root.elem
    def parent: Option[ScopeLocation] = location.up.map(ScopeLocation)

    def newChildScope: ScopeLocation = location.insertChild(Scope.empty)
    def newSiblingScope: ScopeLocation = location.insertRight(Scope.empty).get

    def isEmpty: Boolean = scope.isEmpty

    def localSymbol(name: String): Option[Symbol] = scope.symbol(name)
    def symbol(name: String): Option[Symbol] = localSymbol(name) orElse location.up.flatMap(_.symbol(name))

    def symbolNames: Set[String] = scope.symbolNames

    def importScope(other: Scope): ScopeLocation = location.replace(scope.importScope(other))

    def updateIdentifier(identifier: String, types: TypeSpec, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateIdentifier(identifier, types, positions))
  }
}
import SemanticState.ScopeLocation

case class SemanticState(currentScope: ScopeLocation, typeTable: IdentityMap[ast.Expression, ExpressionTypeInfo]) {
  def scopeTree = currentScope.rootScope

  def newChildScope = copy(currentScope = currentScope.newChildScope)
  def newSiblingScope = copy(currentScope = currentScope.newSiblingScope)
  def popScope = copy(currentScope = currentScope.parent.get)

  def symbol(name: String): Option[Symbol] = currentScope.symbol(name)
  def symbolTypes(name: String) = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importScope(scope: Scope) = copy(currentScope = currentScope.importScope(scope))

  def declareIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec, positions: Set[InputPosition] = Set.empty): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(identifier.name) match {
      case None =>
        Right(updateIdentifier(identifier, possibleTypes, positions + identifier.position))
      case Some(symbol) =>
        Left(SemanticError(s"${identifier.name} already declared", identifier.position, symbol.positions.toSeq: _*))
    }

  def implicitIdentifier(identifier: ast.Identifier, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Right(updateIdentifier(identifier, possibleTypes, Set(identifier.position)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateIdentifier(identifier, inferredTypes, symbol.positions + identifier.position))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${identifier.name} already defined with conflicting type $existingTypes (expected $expectedTypes)",
            identifier.position, symbol.positions.toSeq: _*))
        }
    }

  def ensureIdentifierDefined(identifier: ast.Identifier): Either[SemanticError, SemanticState] =
    this.symbol(identifier.name) match {
      case None         =>
        Left(SemanticError(s"${identifier.name} not defined", identifier.position))
      case Some(symbol) =>
        Right(updateIdentifier(identifier, symbol.types, symbol.positions + identifier.position))
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

  private def updateIdentifier(identifier: ast.Identifier, types: TypeSpec, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateIdentifier(identifier.name, types, locations),
      typeTable = typeTable.updated(identifier, ExpressionTypeInfo(types))
    )
}
