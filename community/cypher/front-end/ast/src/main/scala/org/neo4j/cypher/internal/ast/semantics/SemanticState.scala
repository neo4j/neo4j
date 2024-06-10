/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.semantics

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.ASTAnnotationMap.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.semantics.Scope.DeclarationsAndDependencies
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CrossCompilation
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.TreeElem
import org.neo4j.cypher.internal.util.helpers.TreeZipper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import scala.collection.immutable.HashMap
import scala.language.postfixOps

object SymbolUse {
  def apply(variable: LogicalVariable): SymbolUse = SymbolUse(Ref(variable))
}

/**
 * One use of a variable. This compares variables using reference equality.
 * Any copy of a variable will result in a different SymbolUse according to equals.
 */
final case class SymbolUse(use: Ref[LogicalVariable]) {
  override def toString = s"SymbolUse($uniqueName)"

  /**
   * @return the variable
   */
  def asVariable: LogicalVariable = use.value

  /**
   * @return a name that is unique for this SymbolUse.
   *         A use of a different variable by reference equality will get a different name.
   *         The String includes the name and position of the variable.
   */
  private[semantics] def uniqueName: String = s"${asVariable.name}@${asVariable.position.offset}(${use.toIdString})"

  /**
   * @return The position of the variable and a unique id.
   */
  private[semantics] def positionsAndUniqueIdString: (Int, String) = (asVariable.position.offset, use.toIdString)

  /**
   * @return the name of the variable.
   */
  def name: String = asVariable.name
}

/**
 * A symbol collects the definition and all uses of a variable.
 *
 * All uses are in the same scope or in child scopes of the scope that contains the definition.
 *
 * @param name        the name
 * @param types       the type specification
 * @param definition  the definition
 * @param uses        all uses of the symbol. The definition is not a use.
 * @param unionSymbol if the symbol is only a variable introduced to keep track of UNION return values
 */
final case class Symbol(
  name: String,
  types: TypeSpec,
  definition: SymbolUse,
  uses: Set[SymbolUse],
  unionSymbol: Boolean = false
) {

  /**
   * All references to this symbol. This includes the definition and the uses.
   */
  def references: Set[SymbolUse] = uses + definition

  /**
   * @return the positions and unique IDs of all references.
   */
  private[semantics] def positionsAndUniqueIdString: Set[(Int, String)] = references.map(_.positionsAndUniqueIdString)

  override def toString: String =
    s"${definition.uniqueName}(${uses.map(_.uniqueName).mkString(",")}): ${types.toShortString}"
}

object ExpressionTypeInfo {

  /**
   * Cache ExpressionTypeInfos.
   *
   * By caching ExpressionTypeInfo we can reuse instances that e.g. simply express that an Expression is a Boolean.
   * For large and complex queries this can significantly reduce memory consumption.
   */
  private lazy val cache: Cache[(TypeSpec, Option[TypeSpec]), ExpressionTypeInfo] =
    Caffeine.newBuilder()
      .maximumSize(100)
      .build()

  def apply(specified: TypeSpec, expected: Option[TypeSpec] = None): ExpressionTypeInfo =
    if (CrossCompilation.isTeaVM()) {
      new ExpressionTypeInfo(specified, expected)
    } else {
      cache.get((specified, expected), _ => new ExpressionTypeInfo(specified, expected))
    }
}

final case class ExpressionTypeInfo private (specified: TypeSpec, expected: Option[TypeSpec]) {
  lazy val actual: TypeSpec = expected.fold(specified)(specified intersectOrCoerce)

  def expect(types: TypeSpec): ExpressionTypeInfo = ExpressionTypeInfo(specified, Some(types))
}

object Scope {
  val empty: Scope = Scope(symbolTable = HashMap.empty, children = Vector())

  case class DeclarationsAndDependencies(declarations: Set[SymbolUse], dependencies: Set[SymbolUse])
}

final case class Scope(symbolTable: Map[String, Symbol], children: Seq[Scope]) extends TreeElem[Scope] {

  self =>

  override def updateChildren(newChildren: Seq[Scope]): Scope = copy(children = newChildren)

  def isEmpty: Boolean = symbolTable.isEmpty

  def symbol(name: String): Option[Symbol] = symbolTable.get(name)

  def symbolNames: Set[String] = symbolTable.keySet

  def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): Scope = {
    val otherSymbols = other.symbolTable -- exclude
    copy(symbolTable = symbolTable ++ otherSymbols)
  }

  def updateVariable(
    variable: String,
    types: TypeSpec,
    definition: SymbolUse,
    uses: Set[SymbolUse],
    unionVariable: Boolean = false
  ): Scope =
    copy(symbolTable = symbolTable.updated(variable, Symbol(variable, types, definition, uses, unionVariable)))

  /**
   * All symbol definitions of this scope and its children,
   * grouped by name.
   */
  def allSymbolDefinitions: Map[String, Set[SymbolUse]] = {
    allScopes.foldLeft(Map.empty[String, Set[SymbolUse]]) {
      case (acc0, scope) =>
        scope.symbolDefinitions.foldLeft(acc0) {
          case (acc, symDef) if acc.contains(symDef.name) =>
            acc.updated(symDef.name, acc(symDef.name) + symDef)
          case (acc, symDef) =>
            acc.updated(symDef.name, Set(symDef))
        }
    }
  }

  /**
   * All symbols of this scope and its children,
   * grouped by name.
   */
  def allSymbols: Map[String, Set[Symbol]] = {
    allScopes.foldLeft(Map.empty[String, Set[Symbol]]) {
      case (acc0, scope) =>
        scope.symbolTable.foldLeft(acc0) {
          case (acc, (str, symbol)) if acc.contains(str) =>
            acc.updated(str, acc(str) + symbol)
          case (acc, (str, symbol)) =>
            acc.updated(str, Set(symbol))
        }
    }
  }

  /**
   * All symbols definitions of this scope.
   */
  def symbolDefinitions: Set[SymbolUse] =
    symbolTable.values.map(_.definition).toSet

  /**
   * @return A map from any use (read or definition) of a variable to its definition, in all scopes.
   */
  def allVariableDefinitions: Map[SymbolUse, SymbolUse] =
    allScopes.map(_.variableDefinitions).reduce(_ ++ _)

  /**
   * @return A map from any reference of a variable to its definition, in the current scope.
   */
  def variableDefinitions: Map[SymbolUse, SymbolUse] =
    symbolTable.values.flatMap { symbol =>
      val definition = symbol.definition
      symbol.references.map { use => use -> definition }
    }.toMap

  def allScopes: Seq[Scope] =
    Seq(this) ++ children.flatMap(_.allScopes)

  def toIdString = s"#${Ref(self).toIdString}"

  override def toString: String = {
    val builder = new StringBuilder()
    self.dumpSingle("", builder)
    builder.toString()
  }

  private def dumpSingle(indent: String, builder: StringBuilder): Unit = {
    builder.append(s"$indent${self.toIdString} {${System.lineSeparator}")
    dumpTree(s"  $indent", builder)
    builder.append(s"$indent}${System.lineSeparator}")
  }

  private def dumpTree(indent: String, builder: StringBuilder): Unit = {
    symbolTable.keys.toSeq.sorted.foreach { key =>
      val symbol = symbolTable(key)
      val symbolText = symbol.positionsAndUniqueIdString.toSeq.sorted.map(x => s"${x._1}(${x._2})").mkString(" ")
      builder.append(s"$indent$key: $symbolText${System.lineSeparator}")
    }
    children.foreach { child => child.dumpSingle(indent, builder) }
  }
}

object SemanticState {

  implicit object ScopeZipper extends TreeZipper[Scope]

  val clean: SemanticState = SemanticState(
    Scope.empty.location,
    ASTAnnotationMap.empty,
    ASTAnnotationMap.empty
  )

  implicit class ScopeLocation(val location: ScopeZipper.Location) extends AnyVal {
    def scope: Scope = location.elem

    def rootScope: Scope = location.root.elem

    def root: ScopeLocation = location.root

    def parent: Option[ScopeLocation] = location.up.map(ScopeLocation)

    def newChildScope: ScopeLocation = location.insertChild(Scope.empty)

    def newSiblingScope: ScopeLocation = location.insertRight(Scope.empty).get

    def insertSiblingScope(scope: Scope): ScopeLocation = location.insertRight(scope).get

    def isEmpty: Boolean = scope.isEmpty

    def localSymbol(name: String): Option[Symbol] = scope.symbol(name)

    def symbol(name: String): Option[Symbol] = localSymbol(name) orElse location.up.flatMap(_.symbol(name))

    def symbolNames: Set[String] = scope.symbolNames

    /**
     * Local symbol names of this scope and all parent scopes.
     */
    def availableSymbolDefinitions: Set[SymbolUse] = {
      scope.symbolDefinitions ++ location.up.toSet.flatMap((l: ScopeZipper.Location) => l.availableSymbolDefinitions)
    }

    def importValuesFromScope(other: Scope, exclude: Set[String] = Set.empty): ScopeLocation =
      location.replace(scope.importValuesFromScope(other, exclude))

    def updateVariable(
      variable: String,
      types: TypeSpec,
      definition: SymbolUse,
      uses: Set[SymbolUse],
      unionVariable: Boolean = false
    ): ScopeLocation =
      location.replace(scope.updateVariable(variable, types, definition, uses, unionVariable))

    def declarationsAndDependencies: DeclarationsAndDependencies = {
      val allDefinitions = scope.allSymbolDefinitions.values.flatten.toSet
      val parentDefinitions = parent.get.availableSymbolDefinitions
      val (dependencies, declarations) = allDefinitions.partition { definition =>
        parentDefinitions.contains(definition)
      }
      DeclarationsAndDependencies(declarations, dependencies)
    }
  }

  def recordCurrentScope(node: ASTNode): SemanticCheck =
    (s: SemanticState) => SemanticCheckResult.success(s.recordCurrentScope(node))
}

case class SemanticState(
  currentScope: ScopeLocation,
  typeTable: ASTAnnotationMap[Expression, ExpressionTypeInfo],
  recordedScopes: ASTAnnotationMap[ASTNode, ScopeLocation],
  notifications: Set[InternalNotification] = Set.empty,
  features: Set[SemanticFeature] = Set.empty,
  declareVariablesToSuppressDuplicateErrors: Boolean = true,
  semanticCheckHasRunOnce: Boolean = false,
  targetGraph: Option[GraphReference] =
    None, // used to check different use clause targets given a regular session database
  workingGraph: Option[GraphReference] = None // used for nested check given a composite session database
) {

  def scopeTree: Scope = currentScope.rootScope

  def newChildScope: SemanticState = copy(currentScope = currentScope.newChildScope)

  def newSiblingScope: SemanticState = copy(currentScope = currentScope.newSiblingScope)

  def insertSiblingScope(scope: Scope): SemanticState = copy(currentScope = currentScope.insertSiblingScope(scope))

  def popScope: SemanticState = copy(currentScope = currentScope.parent.get)

  def newBaseScope: SemanticState = copy(currentScope = currentScope.root.newChildScope)

  def symbol(name: String): Option[Symbol] = currentScope.symbol(name)

  def symbolTypes(name: String): TypeSpec = symbol(name).map(_.types).getOrElse(TypeSpec.all)

  def isNode(name: String): Boolean = symbolTypes(name) == CTNode.invariant

  def importValuesFromScope(scope: Scope, exclude: Set[String] = Set.empty): SemanticState =
    copy(currentScope = currentScope.importValuesFromScope(scope, exclude))

  /**
   * @param overriding if `true` then a previous occurrence of that variable is overridden.
   *                   if `false` then a previous occurrence of that variable leads to an error
   */
  def declareVariable(
    variable: LogicalVariable,
    possibleTypes: TypeSpec,
    maybePreviousDeclaration: Option[Symbol] = None,
    overriding: Boolean = false,
    unionVariable: Boolean = false
  ): Either[SemanticError, SemanticState] =
    currentScope.localSymbol(variable.name) match {
      case Some(_) if !overriding =>
        Left(SemanticError(s"Variable `${variable.name}` already declared", variable.position))
      case _ =>
        val (definition, uses) = maybePreviousDeclaration match {
          case Some(previousDeclaration) =>
            (previousDeclaration.definition, previousDeclaration.uses ++ Set(SymbolUse(variable)))
          case None => (SymbolUse(variable), Set.empty[SymbolUse])
        }
        Right(updateVariable(variable, possibleTypes, definition, uses, unionVariable))
    }

  def addNotification(notification: InternalNotification): SemanticState =
    copy(notifications = notifications + notification)

  def implicitVariable(
    variable: LogicalVariable,
    possibleTypes: TypeSpec
  ): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Right(updateVariable(variable, possibleTypes, SymbolUse(variable), Set.empty))

      case Some(symbol) =>
        val inferredTypes = symbol.types intersect possibleTypes
        if (inferredTypes.nonEmpty) {
          Right(updateVariable(variable, inferredTypes, symbol.definition, symbol.uses + SymbolUse(variable)))
        } else {
          val existingTypes = symbol.types.mkString(", ", " or ")
          val expectedTypes = possibleTypes.mkString(", ", " or ")
          Left(SemanticError(
            s"Type mismatch: ${variable.name} defined with conflicting type $existingTypes (expected $expectedTypes)",
            variable.position
          ))
        }
    }

  def ensureVariableDefined(variable: LogicalVariable): Either[SemanticError, SemanticState] =
    this.symbol(variable.name) match {
      case None =>
        Left(SemanticError(s"Variable `${variable.name}` not defined", variable.position))
      case Some(symbol) =>
        Right(updateVariable(variable, symbol.types, symbol.definition, symbol.uses + SymbolUse(variable)))
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

  // Some semantic checks only make sense to be done on the first run before extensive rewriting
  def semanticCheckHasRunOnce(hasRun: Boolean): SemanticState = {
    copy(semanticCheckHasRunOnce = hasRun)
  }

  def expressionType(expression: Expression): ExpressionTypeInfo =
    typeTable.getOrElse(expression, ExpressionTypeInfo(TypeSpec.all))

  private def updateVariable(
    variable: LogicalVariable,
    types: TypeSpec,
    definition: SymbolUse,
    uses: Set[SymbolUse],
    unionVariable: Boolean = false
  ): SemanticState =
    copy(
      currentScope = currentScope.updateVariable(variable.name, types, definition, uses, unionVariable),
      typeTable = typeTable.updated(variable, ExpressionTypeInfo(types))
    )

  def recordCurrentScope(astNode: ASTNode): SemanticState =
    copy(recordedScopes = recordedScopes.updated(astNode, currentScope))

  def scope(astNode: ASTNode): Option[Scope] =
    recordedScopes.get(astNode).map(_.scope)

  def withFeature(feature: SemanticFeature): SemanticState = copy(features = features + feature)

  def recordTargetGraph(targetGraph: GraphReference): SemanticState = copy(targetGraph = Some(targetGraph))

  def recordWorkingGraph(graph: Option[GraphReference]): SemanticState = copy(workingGraph = graph)
}
