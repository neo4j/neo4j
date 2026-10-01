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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.error
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.util.symbols.CypherType

/**
 * Checks for arguments of procedure and function calls against NOT NULL signature types, e.g. the types of
 * parameters declared with Java primitives.
 *
 * T and T NOT NULL are unrelated in the type system, so argument expressions cannot be checked directly against
 * a NOT NULL signature type. They are checked against the nullable variant instead, and null literals are
 * rejected separately.
 */
private[phases] object CallableArgumentChecks {

  def checkArgument(fieldType: CypherType, arg: Expression): SemanticCheck =
    SemanticExpressionCheck.expectType(fieldType.withIsNullable(true).covariant, arg) chain when(
      fieldType.isNotNullContaining && isNullLiteral(arg)
    ) {
      val expectedType = fieldType.description
      error(
        SemanticError.typeMismatch(
          List(expectedType),
          "NULL",
          s"Type mismatch: expected $expectedType but was NULL",
          arg.position
        )
      )
    }

  private def isNullLiteral(arg: Expression): Boolean = arg match {
    // Provided arguments have already been wrapped in CoerceTo by coerceArguments when this runs
    case _: Null              => true
    case CoerceTo(_: Null, _) => true
    case _                    => false
  }
}
