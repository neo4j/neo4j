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
                        graph: Boolean = false,
                        generated: Boolean = false) {
  if (positions.isEmpty)
    throw new InternalException(s"Cannot create empty symbol with name '$name'")

  def uses: Set[SymbolUse] = positions.map { pos => SymbolUse(name, pos) }

  val definition = SymbolUse(name, positions.toSeq.min(InputPosition.byOffset))

  def asGenerated: Symbol = copy(generated = true)

  def withMergedPositions(additionalPositions: Set[InputPosition]): Symbol =
    copy(positions = positions ++ additionalPositions)

  override def toString: String =
    if (graph)
      s"GRAPH ${definition.nameWithPosition}(${positions.map(_.offset).mkString(",")})"
    else
      s"${definition.nameWithPosition}(${positions.map(_.offset).mkString(",")}): ${types.toShortString}"
}

final case class ExpressionTypeInfo(specified: TypeSpec, expected: Option[TypeSpec] = None) {
  lazy val actualUnCoerced: TypeSpec = expected.fold(specified)(specified intersect)
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)
  lazy val wasCoerced: Boolean = actualUnCoerced != actual

  def expect(types: TypeSpec): ExpressionTypeInfo = copy(expected = Some(types))
}

final case class ContextGraphs(source: String, target: String) {

  override def toString: String = s"$source >> $target"

  def format(allGraphs: Set[String]): String = {
    val remainingGraphs = allGraphs -- graphs
    if (remainingGraphs.isEmpty) toString else s"$toString, ${remainingGraphs.mkString(", ")}"
  }

  def graphs = Set(source, target)

  def updated(newSource: Option[String]): ContextGraphs =
    updated(newSource, newSource)

  def updated(newSource: Option[String], newTarget: Option[String]): ContextGraphs =
    copy(source = newSource.getOrElse(source), target = newTarget.getOrElse(target))
}

object Scope {
  val empty = Scope(symbolTable = HashMap.empty, children = Vector())

  def withContext(newContextGraphs: Option[ContextGraphs]): Scope = newContextGraphs match {
    case Some(_) => empty.copy(contextGraphs = newContextGraphs)
    case _ => empty
  }
}

final case class Scope(symbolTable: Map[String, Symbol],
                       children: Seq[Scope],
                       contextGraphs: Option[ContextGraphs] = None
) extends TreeElem[Scope] {

  self =>

  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)
  def symbolNames: Set[String] = symbolTable.keySet

  def variable(name: String): Option[Symbol] = symbolTable.get(name).filter(!_.graph)
  def variableNames: Set[String] = selectSymbolNames(!_.graph)

  def graph(name: String): Option[Symbol] = symbolTable.get(name).filter(_.graph)
  def graphNames: Set[String] = selectSymbolNames(_.graph)

  def selectSymbolNames(f: Symbol => Boolean): Set[String] = symbolTable.collect {
    case ((k, symbol)) if f(symbol) => k
  }.toSet

  def valueSymbolTable: Map[String, Symbol] = symbolTable.collect { case entry if !entry._2.graph => entry }
  def graphSymbolTable: Map[String, Symbol] = symbolTable.collect { case entry if entry._2.graph => entry }

  def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): Scope = {
    val otherSymbols = other.valueSymbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def importGraphsFromScope(other: Scope, exclude: Set[String] = Set.empty): Scope = {
    val otherSymbols = other.graphSymbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def markAsGenerated(variable: String): Scope =
    symbolTable
      .get(variable)
      .map(_.asGenerated)
      .map(symbol => copy(symbolTable = symbolTable.updated(variable, symbol)))
      .getOrElse(this)

  def removeContextGraphs(): Scope =
    copy(contextGraphs = None)

  def updateContextGraphs(initialContextGraphs: ContextGraphs): Scope =
    copy(contextGraphs = Some(initialContextGraphs))

  def updateGraphVariable(variable: String, positions: Set[InputPosition], generated: Boolean = false): Scope =
    copy(
      symbolTable = symbolTable.updated(
        variable,
        Symbol(variable, positions, CTGraphRef, graph = true, generated = generated)
    ))

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
    val contextGraphsInScope = contextGraphs.map(_.toString).map(" /* " ++ _ ++ " */").getOrElse("")
    if (includeId) builder.append(s"$indent${self.toIdString} {$contextGraphsInScope$EOL")
    else builder.append(s"$indent{$contextGraphsInScope$EOL")
    dumpTree(s"  $indent", includeId, builder)
    builder.append(s"$indent}$EOL")
  }

  private def dumpTree(indent: String, includeId: Boolean, builder: StringBuilder): Unit = {
    symbolTable.keys.toSeq.sorted.foreach { key =>
      val symbol = symbolTable(key)
      val generatedText = if (symbol.generated) " (generated)" else ""
      val keyText = if (symbol.graph) s"GRAPH $key$generatedText" else s"$key$generatedText"
      val symbolText = symbol.positions.map(_.toOffsetString).toSeq.sorted.mkString(" ")
      builder.append(s"$indent$keyText: $symbolText$EOL")
    }
    children.foreach { child => child.dumpSingle(indent, includeId, builder) }
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

    def localVariable(name: String): Option[Symbol] = scope.variable(name)
    def variable(name: String): Option[Symbol] = localVariable(name) orElse location.up.flatMap(_.variable(name))
    def variableNames: Set[String] = scope.variableNames

    def localGraph(name: String): Option[Symbol] = scope.graph(name)
    def graph(name: String): Option[Symbol] = localGraph(name) orElse location.up.flatMap(_.graph(name))
    def graphNames: Set[String] = scope.graphNames

    def localContextGraphs: Option[ContextGraphs] = scope.contextGraphs
    def contextGraphs: Option[ContextGraphs] = localContextGraphs orElse location.up.flatMap(_.contextGraphs)

    def sourceGraph: Option[Symbol] = contextGraphs.map(_.source).flatMap(graph)
    def targetGraph: Option[Symbol] = contextGraphs.map(_.target).flatMap(graph)

    def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      location.replace(scope.importValuesFromScope(other, exclude))

    def importGraphsFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      location.replace(scope.importGraphsFromScope(other, exclude))

    def localMarkAsGenerated(name: String): ScopeLocation =
      location.replace(scope.markAsGenerated(name))

    def mergeSymbolPositionsFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      other.symbolTable.values.foldLeft(location) {
        case (loc, sym) if exclude(sym.name) => loc
        case (loc, sym)                      => loc.replace(loc.scope.mergePositions(sym.name, sym.positions))
      }

    def removeContextGraphs(): ScopeLocation =
      location.replace(scope.removeContextGraphs())

    def updateContextGraphs(newContextGraphs: ContextGraphs): ScopeLocation =
      location.replace(scope.updateContextGraphs(newContextGraphs))

    def updateGraph(variable: String, positions: Set[InputPosition], generated: Boolean = false): ScopeLocation =
      location.replace(scope.updateGraphVariable(variable, positions, generated))

    def updateVariable(variable: String, types: TypeSpec, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.updateVariable(variable, types, positions))

    def mergePositions(variable: String, positions: Set[InputPosition]): ScopeLocation =
      location.replace(scope.mergePositions(variable, positions))
  }
}

case class SemanticState(currentScope: ScopeLocation,
                         typeTable: ASTAnnotationMap[ast.Expression, ExpressionTypeInfo],
                         recordedScopes: ASTAnnotationMap[ast.ASTNode, Scope],
                         recordedContextGraphs: ASTAnnotationMap[ast.ASTNode, ContextGraphs] = ASTAnnotationMap.empty,
                         notifications: Set[InternalNotification] = Set.empty,
                         features: Set[SemanticFeature] = Set.empty,
                         initialWith: Boolean = false
                        ) {

  def recogniseInitialWith: SemanticState = copy(initialWith = true)
  def clearInitialWith: SemanticState = if (initialWith) copy(initialWith = false) else this

  def scopeTree: Scope = currentScope.rootScope

  def newChildScope: SemanticState = copy(currentScope = currentScope.newChildScope)
  def newSiblingScope: SemanticState = copy(currentScope = currentScope.newSiblingScope)
  def popScope: SemanticState = copy(currentScope = currentScope.parent.get)

  def symbol(name: String): Option[Symbol] = currentScope.symbol(name)
  def variable(name: String): Option[Symbol] = currentScope.variable(name)
  def graph(name: String): Option[Symbol] = currentScope.graph(name)

  def symbolTypes(name: String): TypeSpec = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def importValuesFromScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.importValuesFromScope(scope, exclude))

  def importGraphsFromScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.importGraphsFromScope(scope, exclude))

  def mergeSymbolPositionsFromScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.mergeSymbolPositionsFromScope(scope, exclude))

  def updateContextGraphs(newContextGraphs: ContextGraphs): Either[SemanticError, SemanticState] = {
    val newScope = currentScope.updateContextGraphs(newContextGraphs)
    if (newScope.sourceGraph.isEmpty)
      Left(SemanticError("No source graph is available in scope", InputPosition.NONE))
    else if (newScope.targetGraph.isEmpty)
      Left(SemanticError("No target graph is available in scope", InputPosition.NONE))
    else
      Right(copy(currentScope = newScope))
  }

  def removeContextGraphs(): Either[SemanticError, SemanticState] = {
    val newScope = currentScope.removeContextGraphs()
    Right(copy(currentScope = newScope))
  }

  def localMarkAsGenerated(variable: ast.Variable): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case None =>
        Left(SemanticError(s"`${variable.name}` cannot be marked as generated - it has not been declared in the local scope", variable.position))
      case Some(symbol) =>
        Right(copy(currentScope = currentScope.localMarkAsGenerated(symbol.name)))
    }

  def declareGraph(variable: ast.Variable, positions: Set[InputPosition] = Set.empty): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case None =>
        Right(updateGraph(variable, positions + variable.position))
      case Some(symbol) if symbol.graph =>
        Left(SemanticError(s"Graph `${variable.name}` already declared", variable.position, symbol.positions.toSeq: _*))
      case Some(symbol) =>
        Left(SemanticError(s"`${variable.name}` already declared as variable", variable.position, symbol.positions.toSeq: _*))
    }

  def declareVariable(variable: ast.Variable, possibleTypes: TypeSpec, positions: Set[InputPosition] = Set.empty): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case None =>
        Right(updateVariable(variable, possibleTypes, positions + variable.position))
      case Some(symbol) if symbol.graph =>
        Left(SemanticError(s"`${variable.name}` already declared as graph", variable.position, symbol.positions.toSeq: _*))
      case Some(symbol) =>
        Left(SemanticError(s"Variable `${variable.name}` already declared", variable.position, symbol.positions.toSeq: _*))
    }

  def addNotification(notification: InternalNotification): SemanticState =
    copy(notifications = notifications + notification)

  def implicitContextGraph(variable: Option[String], position: InputPosition, contextGraphName: String)
  : Either[SemanticError, SemanticState] =
    variable match {
      case Some(name) => implicitGraph(Variable(name)(position))
      case None => Left(SemanticError(s"No $contextGraphName in scope", position))
    }

  def implicitGraph(variable: ast.Variable): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Right(updateGraph(variable, Set(variable.position)))
      case Some(symbol) if symbol.graph =>
        Right(updateGraph(variable, symbol.positions + variable.position, symbol.generated))
      case Some(symbol) =>
        Left(SemanticError(s"`${variable.name}` already declared as variable", variable.position, symbol.positions.toSeq: _*))
    }

  def implicitVariable(variable: ast.Variable, possibleTypes: TypeSpec): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Right(updateVariable(variable, possibleTypes, Set(variable.position)))
      case Some(symbol) if symbol.graph =>
        Left(SemanticError(s"`${variable.name}` already declared as graph", variable.position, symbol.positions.toSeq: _*))
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
      case None  =>
        Left(SemanticError(s"Variable `${variable.name}` not defined", variable.position))
      case Some(symbol) if symbol.graph =>
        Left(SemanticError(s"`${variable.name}` already declared as graph", variable.position, symbol.positions.toSeq: _*))
      case Some(symbol) =>
        Right(updateVariable(variable, symbol.types, symbol.positions + variable.position))
    }

  def ensureGraphDefined(variable: ast.Variable): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None if initialWith =>
        Right(updateGraph(variable, Set(variable.position)))
      case None         =>
        Left(SemanticError(s"Variable `${variable.name}` not defined", variable.position))
      case Some(symbol) if symbol.graph =>
        Right(updateGraph(variable, symbol.positions + variable.position))
      case Some(symbol) =>
        Left(SemanticError(s"`${variable.name}` already declared as variable", variable.position, symbol.positions.toSeq: _*))
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

  def withFeatures(features: SemanticFeature*): SemanticState =
    features.foldLeft(this)(_.withFeature(_))

  def expressionType(expression: ast.Expression): ExpressionTypeInfo = typeTable.getOrElse(expression, ExpressionTypeInfo(TypeSpec.all))

  private def updateGraph(variable: ast.Variable, locations: Set[InputPosition], generated: Boolean = false) =
    copy(
      currentScope = currentScope.updateGraph(variable.name, locations, generated),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(CTGraphRef))
    )

  private def updateVariable(variable: ast.Variable, types: TypeSpec, locations: Set[InputPosition]) =
    copy(
      currentScope = currentScope.updateVariable(variable.name, types, locations),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(types))
    )

  def recordCurrentScope(astNode: ast.ASTNode): SemanticState =
    copy(recordedScopes = recordedScopes.updated(astNode, currentScope.scope))

  def recordCurrentContextGraphs(astNode: ast.ASTNode): SemanticState = {
    val optCurrentContextGraphs = currentScope.contextGraphs
    optCurrentContextGraphs.map(recordedContextGraphs.updated(astNode, _)) match {
      case Some(newRecordedContextGraphs) => copy(recordedContextGraphs = newRecordedContextGraphs)
      case None => this
    }
  }

  def scope(astNode: ast.ASTNode): Option[Scope] =
    recordedScopes.get(astNode)

  def contextGraphs(astNode: ast.ASTNode): Option[ContextGraphs] =
    recordedContextGraphs.get(astNode)

  def withFeature(feature: SemanticFeature): SemanticState = copy(features = features + feature)
}
