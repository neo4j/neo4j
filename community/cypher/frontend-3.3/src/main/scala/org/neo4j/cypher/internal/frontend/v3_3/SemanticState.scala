/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.frontend.v3_3.ast.{ASTAnnotationMap, Variable}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.{TreeElem, TreeZipper}
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CTGraphRef, TypeSpec}

import scala.collection.immutable.HashMap
import scala.language.postfixOps

// A symbol use represents the occurrence of a symbol at a position
final case class SymbolUse(name: String, position: InputPosition) {
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
final case class Symbol(name: String, positions: Set[InputPosition], types: TypeSpec, graph: Boolean = false) {
  if (positions.isEmpty)
    throw new InternalException(s"Cannot create empty symbol with name '$name'")

  def uses = positions.map { pos => SymbolUse(name, pos) }

  val definition = SymbolUse(name, positions.toSeq.min(InputPosition.byOffset))

  override def toString = s"${definition.nameWithPosition}(${positions.map(_.offset).mkString(",")}): ${types.toShortString}"
}

final case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced = actualUnCoerced != actual

  def expect(types: TypeSpec) = copy(expected = Some(types))
}


object Scope {
  val empty = Scope(symbolTable = HashMap.empty, children = Vector())
}

final case class Scope(symbolTable: Map[String, Symbol], children: Seq[Scope],
                       setSourceGraph: Option[String] = None, setTargetGraph: Option[String] = None
) extends TreeElem[Scope] {

  self =>

  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbolNames: Set[String] = symbolTable.keySet

  def selectSymbolNames(f: Symbol => Boolean): Set[String] = symbolTable.collect {
    case ((k, symbol)) if f(symbol) => k
  }.toSet

  def importScope(other: Scope, exclude: Set[String] = Set.empty) = {
    val otherSymbols = other.symbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def updateGraphVariable(variable: String, positions: Set[InputPosition]): Scope =
    copy(symbolTable = symbolTable.updated(variable, Symbol(variable, positions, CTGraphRef, graph = true)))

  def updateSetContextGraphs(newSource: Option[String], newTarget: Option[String]) =
    copy(setSourceGraph = newSource orElse setSourceGraph, setTargetGraph = newTarget orElse setTargetGraph)

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
      val keyText =
        setSourceGraph.flatMap(name => if (name == key) Some(s"$key >>") else None).orElse(
        setTargetGraph.flatMap(name => if (name == key) Some(s">> $key") else None)
      ).getOrElse(key)
      val symbolText = symbol.positions.map(_.toOffsetString).toSeq.sorted.mkString(" ")
      builder.append(s"$indent$keyText: $symbolText$EOL")
    }
    children.foreach { child => child.dumpSingle(indent, builder) }
  }
}

object SemanticState {
  implicit object ScopeZipper extends TreeZipper[Scope]

  def withFeatures(features: SemanticFeature*): SemanticState =
    features.foldLeft(SemanticState.clean)(_.withFeature(_))

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

    def graphScope: GraphScope = GraphScope(sourceGraph, targetGraph, graphs)

    def graphs: Set[String] = scope.selectSymbolNames(_.graph) ++ location.up.map(_.graphs).getOrElse(Set.empty)
    def sourceGraph: Option[Symbol] = localSourceGraph orElse location.up.flatMap(_.sourceGraph)
    def targetGraph: Option[Symbol] = localTargetGraph orElse location.up.flatMap(_.targetGraph)
    def localSourceGraph: Option[Symbol] = scope.setSourceGraph.flatMap(symbol).filter(_.graph)
    def localTargetGraph: Option[Symbol] = scope.setSourceGraph.flatMap(symbol).filter(_.graph)

    def symbolNames: Set[String] = scope.symbolNames

    def importScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation = location.replace(scope.importScope(other, exclude))

    def mergeScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation = other.symbolTable.values.foldLeft(location) {
      case (loc, sym) if exclude(sym.name) => loc
      case (loc, sym)                      => loc.replace(loc.scope.mergePositions(sym.name, sym.positions))
    }

    def updateSetContextGraphs(newSource: Option[String], newTarget: Option[String]): ScopeLocation =
      location.replace(scope.updateSetContextGraphs(newSource, newTarget))

    def updateGraphVariable(variable: String, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateGraphVariable(variable, positions))

    def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateVariable(variable, types, positions))

    def mergePositions(variable: String, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.mergePositions(variable, positions))
  }
}

final case class GraphScope(source: Option[Symbol], target: Option[Symbol], graphs: Set[String]) {
  override def toString = {
    val sourceName = source.map(_.name).getOrElse("-")
    val targetName = target.map(_.name).getOrElse("-")
    val graphList = (graphs -- Set(sourceName, targetName)).toSeq.sorted
    if (graphList.isEmpty)
      s"GraphScope(${sourceName} >> ${targetName})"
    else
      s"GraphScope(${sourceName} >> ${targetName}, ${graphList.mkString(", ")})"
  }
}


case class SemanticState(currentScope: ScopeLocation,
                         typeTable: ASTAnnotationMap[ast.Expression, ExpressionTypeInfo],
                         recordedScopes: ASTAnnotationMap[ast.ASTNode, Scope],
                         recordedGraphScopes: ASTAnnotationMap[ast.ASTNode, GraphScope] = ASTAnnotationMap.empty,
                         notifications: Set[InternalNotification] = Set.empty,
                         features: Set[SemanticFeature] = Set.empty
                        ) {
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

  def updateSetContextGraphs(newSource: Option[Variable], newTarget: Option[Variable]): Either[SemanticError, SemanticState] = {
    val newScope= currentScope.updateSetContextGraphs(newSource.map(_.name), newTarget.map(_.name))
    if (newScope.sourceGraph.isEmpty)
      Left(SemanticError("No source graph is available in scope", InputPosition.NONE))
    else if (newScope.targetGraph.isEmpty)
      Left(SemanticError("No target graph is available in scope", InputPosition.NONE))
    else
      Right(copy(currentScope = newScope))
  }

  def declareGraphVariable(variable: ast.Variable, positions: Set[InputPosition] = Set.empty): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case None =>
        Right(updateGraphVariable(variable, positions + variable.position))
      case Some(symbol) =>
        Left(SemanticError(s"Variable `${variable.name}` already declared", variable.position, symbol.positions.toSeq: _*))
    }

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

  private def updateGraphVariable(variable: ast.Variable, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateGraphVariable(variable.name, locations),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(CTGraphRef))
    )

  private def updateVariable(variable: ast.Variable, types: TypeSpec, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateVariable(variable.name, types, locations),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(types))
    )

  def recordCurrentScope(astNode: ast.ASTNode): SemanticState =
    copy(recordedScopes = recordedScopes.updated(astNode, currentScope.scope))

  def recordCurrentGraphScope(astNode: ast.ASTNode): SemanticState =
    copy(recordedGraphScopes = recordedGraphScopes.updated(astNode, currentScope.graphScope))

  def scope(astNode: ast.ASTNode): Option[Scope] =
    recordedScopes.get(astNode)

  def graphScope(astNode: ast.ASTNode): Option[GraphScope] =
    recordedGraphScopes.get(astNode)

  def withFeature(feature: SemanticFeature): SemanticState = copy(features = features + feature)
}
