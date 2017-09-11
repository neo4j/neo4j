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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, SemanticCheckResult, SemanticError, SemanticState, SymbolUse}

case class Variable(name: String)(val position: InputPosition) extends Expression {

  def toSymbolUse = SymbolUse(name, position)

  // check the variable is defined and, if not, define it so that later errors are suppressed
  //
  // this is used in expressions; in graphs we must make sure to sem check variables explicitly (!)
  //
  def semanticCheck(ctx: SemanticContext): (SemanticState) => SemanticCheckResult = s => this.ensureVariableDefined()(s) match {
    case Right(ss) => SemanticCheckResult.success(ss)
    case Left(error) => declareVariable(CTAny.covariant)(s) match {
        // if the variable is a graph, declaring it will fail
      case Right(ss) => SemanticCheckResult.error(ss, error)
      case Left(_error) => SemanticCheckResult.error(s, _error)
    }
  }

  // double-dispatch helpers

  def declareGraph: (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareGraph(this)

  def declareGraphMarkedAsGenerated: SemanticCheck = {
    val declare = (_: SemanticState).declareGraph(this)
    val mark = (_: SemanticState).localMarkAsGenerated(this)
    declare chain mark
  }

  def implicitGraph: (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).implicitGraph(this)

  def declareVariable(possibleTypes: TypeSpec): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).declareVariable(this, possibleTypes)

  def declareVariable(typeGen: SemanticState => TypeSpec, positions: Set[InputPosition] = Set.empty)
  : (SemanticState) => Either[SemanticError, SemanticState] =
    (s: SemanticState) => s.declareVariable(this, typeGen(s), positions)

  def implicitVariable(possibleType: CypherType): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).implicitVariable(this, possibleType)

  def ensureGraphDefined(): SemanticCheck = {
    val ensured = (_: SemanticState).ensureGraphDefined(this)
    ensured chain expectType(CTGraphRef.covariant)
  }

  def ensureVariableDefined(): (SemanticState) => Either[SemanticError, SemanticState] =
    (_: SemanticState).ensureVariableDefined(this)

  def copyId: Variable = copy()(position)

  def renameId(newName: String): Variable = copy(name = newName)(position)

  def bumpId: Variable = copy()(position.bumped())

  def asAlias: AliasedReturnItem = AliasedReturnItem(this.copyId, this.copyId)(this.position)

  override def asCanonicalStringVal: String = name
}

object Variable {
  implicit val byName: Ordering[Variable] =
    Ordering.by { (variable: Variable) =>
      (variable.name, variable.position)
    }(Ordering.Tuple2(implicitly[Ordering[String]], InputPosition.byOffset))
}
