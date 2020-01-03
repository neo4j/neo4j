/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_5.ast.semantics

import org.neo4j.cypher.internal.v3_5.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.v3_5.expressions.{Expression, LogicalVariable, Variable}
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.helpers.{TreeElem, TreeZipper}
import org.neo4j.cypher.internal.v3_5.util.symbols.{TypeSpec, _}

import scala.collection.immutable.HashMap
import scala.language.postfixOps

object SymbolUse {
  def apply(variable: LogicalVariable): SymbolUse = SymbolUse(variable.name, variable.position)
}

// A symbol use represents the occurrence of a symbol at a position
final case class SymbolUse(name: String, position: InputPosition) {
  override def toString = s"SymbolUse($nameWithPosition)"

  def asVariable: Variable = Variable(name)(position)

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
final case class Symbol(name: String, positions: Set[InputPosition],
                        types: TypeSpec,
                        generated: Boolean = false) {
  if (positions.isEmpty)
    throw new InternalException(s"Cannot create empty symbol with name '$name'")

  def uses: Set[SymbolUse] = positions.map { pos => SymbolUse(name, pos) }

  val definition = SymbolUse(name, positions.toSeq.min(InputPosition.byOffset))

  def asGenerated: Symbol = copy(generated = true)

  def withMergedPositions(additionalPositions: Set[InputPosition]): Symbol =
    copy(positions = positions ++ additionalPositions)

  override def toString: String =
    s"${definition.nameWithPosition}(${positions.map(_.offset).mkString(",")}): ${types.toShortString}"
}

final case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced: TypeSpec = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced: Boolean = actualUnCoerced != actual

  def expect(types: TypeSpec): ExpressionTypeInfo = copy(expected = Some(types))
}

object Scope {
  val empty = Scope(symbolTable = HashMap.empty, children = Vector())
}

final case class Scope(symbolTable: Map[String, Symbol],
                       children: Seq[Scope]) extends TreeElem[Scope] {

  self =>

  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbolNames: Set[String] = symbolTable.keySet

  def valueSymbolTable: Map[String, Symbol] = symbolTable

  def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): Scope = {
    val otherSymbols = other.valueSymbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def markAsGenerated(variable: String): Scope =
    symbolTable
      .get(variable)
      .map(_.asGenerated)
      .map(symbol => copy(symbolTable = symbolTable.updated(variable, symbol)))
      .getOrElse(this)

  def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]): Scope =
    copy(symbolTable = symbolTable.updated(variable, Symbol(variable, positions, types)))

  def mergePositions(variable: String, positions: Set[InputPosition]): Scope =
    symbolTable.get(variable) match {
      case Some(symbol) => copy(symbolTable = symbolTable.updated(variable, symbol.withMergedPositions(positions)))
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

  override def toString: String =
    format(includeId = true)

  def toStringWithoutId: String =
    format(includeId = false)

  private def format(includeId: Boolean): String = {
    val builder = new StringBuilder()
    self.dumpSingle("", includeId, builder)
    builder.toString()
  }

  import scala.compat.Platform.EOL

  private def dumpSingle(indent: String, includeId: Boolean, builder: StringBuilder): Unit = {
    if (includeId) builder.append(s"$indent${self.toIdString} {$EOL")
    else builder.append(s"$indent{$EOL")
    dumpTree(s"  $indent", includeId, builder)
    builder.append(s"$indent}$EOL")
  }

  private def dumpTree(indent: String, includeId: Boolean, builder: StringBuilder): Unit = {
    symbolTable.keys.toSeq.sorted.foreach { key =>
      val symbol = symbolTable(key)
      val generatedText = if (symbol.generated) " (generated)" else ""
      val keyText = s"$key$generatedText"
      val symbolText = symbol.positions.map(_.toOffsetString).toSeq.sorted.mkString(" ")
      builder.append(s"$indent$keyText: $symbolText$EOL")
    }
    children.foreach { child => child.dumpSingle(indent, includeId, builder) }
  }
}

object SemanticState {

  implicit object ScopeZipper extends TreeZipper[Scope]

  def withStartingVariables(variables: (String, CypherType)*) =
    SemanticState(
      Scope.empty.copy(variables.toMap.map {
        case (name, t) =>
          name -> Symbol(name, Set(InputPosition.NONE), TypeSpec.exact(t))
      }).location,
      ASTAnnotationMap.empty,
      ASTAnnotationMap.empty
    )

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

    def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      location.replace(scope.importValuesFromScope(other, exclude))

    def localMarkAsGenerated(name: String): ScopeLocation =
      location.replace(scope.markAsGenerated(name))

    def mergeSymbolPositionsFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      other.symbolTable.values.foldLeft(location) {
        case (loc, sym) if exclude(sym.name) => loc
        case (loc, sym) =>
          val locWithMergedPos = loc.replace(loc.scope.mergePositions(sym.name, sym.positions))
          val leftWithMergedPos = loc.leftList.map(_.mergePositions(sym.name, sym.positions))
          locWithMergedPos.replaceLeftList(leftWithMergedPos)
      }

    def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateVariable(variable, types, positions))

    def mergePositions(variable: String, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.mergePositions(variable, positions))
  }

  def recordCurrentScope(node: ASTNode): SemanticCheck =
    s => SemanticCheckResult.success(s.recordCurrentScope(node))
}

case class SemanticState(currentScope: ScopeLocation,
                         typeTable: ASTAnnotationMap[Expression, ExpressionTypeInfo],
                         recordedScopes: ASTAnnotationMap[ASTNode, Scope],
                         notifications: Set[InternalNotification] = Set.empty,
                         features: Set[SemanticFeature] = Set.empty,
                         initialWith: Boolean = false,
                         declareVariablesToSuppressDuplicateErrors: Boolean = true,
                         cypher9ComparabilitySemantics: Boolean = false) {

  def recogniseInitialWith: SemanticState = copy(initialWith = true)

  def clearInitialWith: SemanticState = if (initialWith) copy(initialWith = false) else this

  def scopeTree: Scope = currentScope.rootScope

  def newChildScope: SemanticState = copy(currentScope = currentScope.newChildScope)

  def newSiblingScope: SemanticState = copy(currentScope = currentScope.newSiblingScope)

  def popScope: SemanticState = copy(currentScope = currentScope.parent.get)

  def symbol(name: String): Option[Symbol] = currentScope.symbol(name)

  def symbolTypes(name: String): TypeSpec = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importValuesFromScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.importValuesFromScope(scope, exclude))

  def withCypher9ComparabilitySemantics(cypher9ComparabilitySemantics: Boolean): SemanticState = copy(cypher9ComparabilitySemantics = cypher9ComparabilitySemantics)

  /**
    * @param overriding if `true` then a previous occurrence of that variable is overridden.
    *                   if `false` then a previous occurrence of that variable leads to an error
    */
  def declareVariable(variable: LogicalVariable,
                      possibleTypes: TypeSpec,
                      positions: Set[InputPosition] = Set.empty,
                      overriding: Boolean = false): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case Some(symbol) if !overriding =>
        Left(SemanticError(s"Variable `${variable.name}` already declared", variable.position, symbol.positions.toSeq: _*))
      case _ =>
        Right(updateVariable(variable, possibleTypes, positions + variable.position))
    }


  def addNotification(notification: InternalNotification): SemanticState =
    copy(notifications = notifications + notification)

  def implicitVariable(variable: LogicalVariable, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Right(updateVariable(variable, possibleTypes, Set(variable.position)))
      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateVariable(variable, inferredTypes, symbol.positions + variable.position))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${variable.name} defined with conflicting type $existingTypes (expected $expectedTypes)",
            variable.position, symbol.positions.toSeq: _*))
        }
    }

  def ensureVariableDefined(variable: LogicalVariable): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Left(SemanticError(s"Variable `${variable.name}` not defined", variable.position))
      case Some(symbol) =>
        Right(updateVariable(variable, symbol.types, symbol.positions + variable.position))
    }

  def specifyType(expression: Expression, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    expression match {
      case variable: Variable =>
        implicitVariable(variable, possibleTypes)
      case _ =>
        Right(copy(typeTable = typeTable.updated(expression, ExpressionTypeInfo(possibleTypes))))
    }

  def expectType(expression: Expression, possibleTypes: TypeSpec): (SemanticState, TypeSpec) = {
    val expType = expressionType(expression)
    val updated = expType.expect(possibleTypes)
    (copy(typeTable = typeTable.updated(expression, updated)), updated.actual)
  }

  def withFeatures(features: SemanticFeature*): SemanticState =
    features.foldLeft(this)(_.withFeature(_))

  def expressionType(expression: Expression): ExpressionTypeInfo = typeTable.getOrElse(expression, ExpressionTypeInfo(TypeSpec.all))

  private def updateVariable(variable: LogicalVariable, types: TypeSpec, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateVariable(variable.name, types, locations),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(types))
    )

  def recordCurrentScope(astNode: ASTNode): SemanticState =
    copy(recordedScopes = recordedScopes.updated(astNode, currentScope.scope))

  def scope(astNode: ASTNode): Option[Scope] =
    recordedScopes.get(astNode)

  def withFeature(feature: SemanticFeature): SemanticState = copy(features = features + feature)
}
