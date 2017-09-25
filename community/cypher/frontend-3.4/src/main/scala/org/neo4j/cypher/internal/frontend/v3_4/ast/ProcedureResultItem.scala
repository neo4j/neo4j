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

import org.neo4j.cypher.internal.aux.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.aux.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.{ProcedureOutput, Variable}

object ProcedureResultItem {
  def apply(output: ProcedureOutput, variable: Variable)(position: InputPosition): ProcedureResultItem =
    ProcedureResultItem(Some(output), variable)(position)

  def apply(variable: Variable)(position: InputPosition): ProcedureResultItem =
    ProcedureResultItem(None, variable)(position)
}

case class ProcedureResultItem(output: Option[ProcedureOutput], variable: Variable)(val position: InputPosition)
  extends ASTNode with SemanticAnalysisTooling {

  val outputName: String = output.map(_.name).getOrElse(variable.name)

  def semanticCheck: SemanticCheck =
    // This is needed to prevent the initial round of semantic checking from failing with type errors
    // when procedure signatures have not yet been resolved
    declareVariable(variable, TypeSpec.all)

  def semanticCheck(types: Map[String, CypherType]): SemanticCheck =
    types
      .get(outputName)
      .map { typ => declareVariable(variable, typ): SemanticCheck }
      .getOrElse(error(s"Unknown procedure output: `$outputName`", position))
}
