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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

sealed trait Options extends ASTNode {
  def checkOptionsForSchema(schemaString: String): SemanticCheck
}

case object NoOptions extends Options {
  val position: InputPosition = InputPosition.NONE

  override def checkOptionsForSchema(schemaString: String): SemanticCheck = SemanticCheck.success
}

case class OptionsParam(parameter: Parameter)(val position: InputPosition) extends Options {
  override def checkOptionsForSchema(schemaString: String): SemanticCheck = SemanticCheck.success
}

case class OptionsMap(map: Map[String, Expression])(val position: InputPosition) extends Options {

  // The validation of the values (provider, config keys and config values) are done at runtime.
  override def checkOptionsForSchema(schemaString: String): SemanticCheck =
    SemanticCheck.fromContext { context =>
      val (validOptions, errorMessageOverride) =
        if (context.cypherVersion == CypherVersion.Cypher5) {
          (
            Seq("indexProvider", "indexConfig"),
            Some(
              s"Failed to create $schemaString: Invalid option provided, valid options are `indexProvider` and `indexConfig`."
            )
          )
        } else {
          (Seq("indexConfig"), None)
        }
      val invalidKeys = map.view.filterKeys(k =>
        !validOptions.exists(_.equalsIgnoreCase(k))
      )

      if (invalidKeys.isEmpty) {
        // Make sure the values are at least semantically correct
        map.foldLeft(SemanticCheck.success) {
          case (acc, (_, expr)) => acc chain SemanticExpressionCheck.check(Expression.SemanticContext.Results, expr)
        }
      } else {
        SemanticCheck.error(SemanticError.invalidOption(
          invalidKeys.keys.mkString(" and "),
          validOptions,
          errorMessageOverride,
          position
        ))
      }
    }
}
