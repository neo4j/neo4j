/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticError
import org.neo4j.cypher.internal.util.v3_4.symbols.TypeSpec
import org.neo4j.cypher.internal.util.v3_4.{DummyPosition, InputPosition}
import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.v3_4.expressions.Expression.SemanticContext

case class ErrorExpression(
                            error: SemanticError,
                            possibleTypes: TypeSpec,
                            position: InputPosition = DummyPosition(0)
                          ) extends Expression

case class CustomExpression(
                             semanticCheck: (SemanticContext, CustomExpression) => SemanticCheck,
                             position: InputPosition = DummyPosition(0)
                           ) extends Expression
