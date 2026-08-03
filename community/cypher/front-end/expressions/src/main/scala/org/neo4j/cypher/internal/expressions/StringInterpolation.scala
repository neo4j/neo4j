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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

case class StringInterpolation(
  stringParts: Seq[Expression],
  expressions: Seq[Expression]
)(val position: InputPosition) extends Expression {

  override def isConstantForQuery: Boolean = (stringParts ++ expressions).forall(_.isConstantForQuery)

  override def asCanonicalStringVal: String = {
    val sb = new StringBuilder
    stringParts.zipWithIndex.foreach { case (part, i) =>
      sb.append(part.asCanonicalStringVal)
      if (i < expressions.size) sb.append(s"{${expressions(i).asCanonicalStringVal}}")
    }
    sb.toString()
  }

  def getResultingExpression: Expression = (stringParts, expressions) match {
    case (Seq(), Seq())                      => StringLiteral("")(position.withInputLength(2))
    case (Seq(string: StringLiteral), Seq()) => string
    case (_, _)                              => this
  }
}
