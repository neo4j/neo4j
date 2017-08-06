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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheckResult, SemanticState, SymbolUse}

case class Variable(name: String)(val position: InputPosition) extends Expression {

  def toSymbolUse = SymbolUse(name, position)

  // check the variable is defined and, if not, define it so that later errors are suppressed
  def semanticCheck(ctx: SemanticContext) = s => this.ensureDefined()(s) match {
    case Right(ss) => SemanticCheckResult.success(ss)
    case Left(error) => SemanticCheckResult.error(declare(CTAny.covariant)(s).right.get, error)
  }

  // double-dispatch helpers
  def declareGraph =
    (_: SemanticState).declareGraphVariable(this)

  def declare(possibleTypes: TypeSpec) =
    (_: SemanticState).declareVariable(this, possibleTypes)

  def declare(typeGen: SemanticState => TypeSpec, positions: Set[InputPosition] = Set.empty) =
    (s: SemanticState) => s.declareVariable(this, typeGen(s), positions)

  def implicitDeclaration(possibleType: CypherType) =
    (_: SemanticState).implicitVariable(this, possibleType)

  def ensureDefined() =
    (_: SemanticState).ensureVariableDefined(this)

  def copyId = copy()(position)

  def renameId(newName: String) = copy(name = newName)(position)

  def bumpId = copy()(position.bumped())

  def asAlias = AliasedReturnItem(this.copyId, this.copyId)(this.position)

  override def asCanonicalStringVal: String = name
}

object Variable {
  implicit val byName =
    Ordering.by { (variable: Variable) =>
      (variable.name, variable.position)
    }(Ordering.Tuple2(implicitly[Ordering[String]], InputPosition.byOffset))
}
