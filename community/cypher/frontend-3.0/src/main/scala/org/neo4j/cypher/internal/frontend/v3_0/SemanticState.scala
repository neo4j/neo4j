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
package org.neo4j.cypher.internal.frontend.v3_0

import org.neo4j.cypher.internal.frontend.v3_0.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.frontend.v3_0.ast.{ASTAnnotationMap, Variable}
import org.neo4j.cypher.internal.frontend.v3_0.helpers.{TreeElem, TreeZipper}
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_0.symbols.TypeSpec

import scala.collection.immutable.HashMap
import scala.language.postfixOps

// A symbol use represents the occurrence of a symbol at a position
case class SymbolUse(name: String, position: InputPosition) {
  override def toString = s"SymbolUse($nameWithPosition)"

  def asVariable = Variable(name)(position)
  def nameWithPosition = s"$name@${position.toOffsetString}"
}

// A symbol collects all uses of a position within the current scope and
// up to the originally defining scope together with type information
//
// (s1 n1 (s2 ... (s3 n2 (s4 ... n3)) =>
//
// s1.localSymbol(n) = Symbol(n, Seq(1), type(n))
// s3.localSymbol(n) = Symbol(n, Seq(1, 2), type(n))
// s4.localSymbol(n) = Symbol(n, Seq(1, 2, 3), type(n))
//
case class Symbol(name: String, positions: Set[InputPosition], types: TypeSpec) {
  if (positions.isEmpty)
    throw new InternalException(s"Cannot create empty symbol with name '$name'")

  def uses = positions.map { pos => SymbolUse(name, pos) }

  val definition = SymbolUse(name, positions.toSeq.min(InputPosition.byOffset))

  override def toString = s"${definition.nameWithPosition}(${positions.map(_.offset).mkString(",")}): ${types.toShortString}"
}

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

  self =>

  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbolNames: Set[String] = symbolTable.keySet

  def importScope(other: Scope, exclude: Set[String] = Set.empty) = {
    val otherSymbols = other.symbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]) =
    copy(symbolTable = symbolTable.updated(variable, Symbol(variable, positions, types)))

  def mergePositions(variable: String, positions: Set[InputPosition]) = symbolTable.get(variable) match {
    case Some(symbol) => copy(symbolTable = symbolTable.updated(variable, symbol.copy(positions = positions ++ symbol.positions)))
    case None => self
  }

  def allSymbolDefinitions: Map[String, Set[SymbolUse]] = {
    val allScopes1 = allScopes
    allScopes1.foldLeft(Map.empty[String, Set[SymbolUse]]) {
      case (acc0, scope) =>
        scope.symbolDefinitions.foldLeft(acc0) {
          case (acc, symDef) if acc.contains(symDef.name) =>
            acc.updated(symDef.name, acc(symDef.name) + symDef)
          case (acc, symDef) =>
            acc.updated(symDef.name, Set(symDef))
        }
    }
  }

  def symbolDefinitions: Set[SymbolUse] =
    symbolTable.values.map(_.definition).toSet

  def allVariableDefinitions: Map[SymbolUse, SymbolUse] =
    allScopes.map(_.variableDefinitions).reduce(_ ++ _)

  def variableDefinitions: Map[SymbolUse, SymbolUse] =
    symbolTable.values.flatMap { symbol =>
      val name = symbol.name
      val definition = symbol.definition
      symbol.positions.map { pos => SymbolUse(name, pos) -> definition }
    }.toMap

  def allScopes: Seq[Scope] =
    Seq(this) ++ children.flatMap(_.allScopes)

  def toIdString = s"#${Ref(self).toIdString}"

  override def toString: String = {
    val builder = new StringBuilder()
    self.dumpSingle("", builder)
    builder.toString()
  }
  import scala.compat.Platform.EOL

  private def dumpSingle(indent: String, builder: StringBuilder): Unit = {
    builder.append(s"$indent${self.toIdString} {$EOL")
    dumpTree(s"  $indent", builder)
    builder.append(s"$indent}$EOL")
  }

  private def dumpTree(indent: String, builder: StringBuilder): Unit = {
    symbolTable.keys.toSeq.sorted.foreach { key =>
      val symbol = symbolTable(key)
      val symbolText = symbol.positions.map(_.toOffsetString).toSeq.sorted.mkString(" ")
      builder.append(s"$indent$key: $symbolText$EOL")
    }
    children.foreach { child => child.dumpSingle(indent, builder) }
  }
}


object SemanticState {
  implicit object ScopeZipper extends TreeZipper[Scope]

  val clean = SemanticState(Scope.empty.location, ASTAnnotationMap.empty, ASTAnnotationMap.empty)

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

    def importScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation = location.replace(scope.importScope(other, exclude))

    def mergeScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation = other.symbolTable.values.foldLeft(location) {
      case (loc, sym) if exclude(sym.name) => loc
      case (loc, sym)                      => loc.replace(loc.scope.mergePositions(sym.name, sym.positions))
    }

    def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateVariable(variable, types, positions))

    def mergePositions(variable: String, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.mergePositions(variable, positions))
  }
}

case class SemanticState(currentScope: ScopeLocation,
                         typeTable: ASTAnnotationMap[ast.Expression, ExpressionTypeInfo],
                         recordedScopes: ASTAnnotationMap[ast.ASTNode, Scope],
                         notifications: Set[InternalNotification] = Set.empty) {
  def scopeTree = currentScope.rootScope

  def newChildScope = copy(currentScope = currentScope.newChildScope)
  def newSiblingScope = copy(currentScope = currentScope.newSiblingScope)
  def popScope = copy(currentScope = currentScope.parent.get)

  def symbol(name: String): Option[Symbol] = currentScope.symbol(name)
  def symbolTypes(name: String) = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.importScope(scope, exclude))

  def mergeScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.mergeScope(scope, exclude))

  def declareVariable(variable: ast.Variable, possibleTypes: TypeSpec, positions: Set[InputPosition] = Set.empty): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case None =>
        Right(updateVariable(variable, possibleTypes, positions + variable.position))
      case Some(symbol) =>
        Left(SemanticError(s"Variable `${variable.name}` already declared", variable.position, symbol.positions.toSeq: _*))
    }

  def addNotification(notification: InternalNotification) = copy(notifications = notifications + notification)

  def implicitVariable(variable: ast.Variable, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None         =>
        Right(updateVariable(variable, possibleTypes, Set(variable.position)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateVariable(variable, inferredTypes, symbol.positions + variable.position))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${variable.name} already defined with conflicting type $existingTypes (expected $expectedTypes)",
            variable.position, symbol.positions.toSeq: _*))
        }
    }

  def ensureVariableDefined(variable: ast.Variable): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None         =>
        Left(SemanticError(s"Variable `${variable.name}` not defined", variable.position))
      case Some(symbol) =>
        Right(updateVariable(variable, symbol.types, symbol.positions + variable.position))
    }

  def specifyType(expression: ast.Expression, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    expression match {
      case variable: ast.Variable =>
        implicitVariable(variable, possibleTypes)
      case _                          =>
        Right(copy(typeTable = typeTable.updated(expression, ExpressionTypeInfo(possibleTypes))))
    }

  def expectType(expression: ast.Expression, possibleTypes: TypeSpec): (SemanticState, TypeSpec) = {
    val expType = expressionType(expression)
    val updated = expType.expect(possibleTypes)
    (copy(typeTable = typeTable.updated(expression, updated)), updated.actual)
  }

  def expressionType(expression: ast.Expression): ExpressionTypeInfo = typeTable.getOrElse(expression, ExpressionTypeInfo(TypeSpec.all))

  private def updateVariable(variable: ast.Variable, types: TypeSpec, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateVariable(variable.name, types, locations),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(types))
    )

  def noteCurrentScope(astNode: ast.ASTNode): SemanticState =
    copy(recordedScopes = recordedScopes.updated(astNode, currentScope.scope))

  def scope(astNode: ast.ASTNode): Option[Scope] =
    recordedScopes.get(astNode)
}
