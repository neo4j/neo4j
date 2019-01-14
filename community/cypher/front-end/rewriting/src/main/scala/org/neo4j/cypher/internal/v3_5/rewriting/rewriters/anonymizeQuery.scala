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
package org.neo4j.cypher.internal.v3_5.rewriting.rewriters

import org.neo4j.cypher.internal.v3_5.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.{Rewriter, bottomUp}

/**
  * Anonymizer which renames tokens and cypher parts using some scheme. All renames have to be reproducible and
  * unique to create a valid query.
  *
  * The intended usage of this rewriter is for anonymizing queries before storage, to avoid retaining domain specific
  * information which could harm the operation or integrity of the original cypher deployment. This anonymization would
  * execucted by 1) parsing the query, 2) running the rewriter, and 3) writing the query back to string form
  * using the Prettifier.
  */
trait Anonymizer {
  def variable(name: String): String
  def unaliasedReturnItemName(anonymizedExpression: Expression, input: String): String
  def label(name: String): String
  def relationshipType(name: String): String
  def propertyKey(name: String): String
  def parameter(name: String): String
  def literal(value: String): String
}

case class anonymizeQuery(anonymizer: Anonymizer) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case v: Variable => Variable(anonymizer.variable(v.name))(v.position)
    case x: UnaliasedReturnItem => UnaliasedReturnItem(x.expression, anonymizer.unaliasedReturnItemName(x.expression, x.inputText))(x.position)
    case x: LabelName => LabelName(anonymizer.label(x.name))(x.position)
    case x: RelTypeName => RelTypeName(anonymizer.relationshipType(x.name))(x.position)
    case x: PropertyKeyName => PropertyKeyName(anonymizer.propertyKey(x.name))(x.position)
    case x: Parameter => Parameter(anonymizer.parameter(x.name), x.parameterType)(x.position)
    case x: StringLiteral => StringLiteral(anonymizer.literal(x.value))(x.position)
  })
}
