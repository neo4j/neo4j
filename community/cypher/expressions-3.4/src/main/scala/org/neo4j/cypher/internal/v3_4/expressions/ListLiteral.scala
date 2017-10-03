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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.aux.v3_4.InputPosition

case class ListLiteral(expressions: Seq[Expression])(val position: InputPosition) extends Expression {

  def map(f: Expression => Expression) = copy(expressions = expressions.map(f))(position)

  override def asCanonicalStringVal: String = expressions.map(_.asCanonicalStringVal).mkString("[", ", ", "]")
}

case class ListSlice(list: Expression, from: Option[Expression], to: Option[Expression])(val position: InputPosition)
  extends Expression

case class ContainerIndex(expr: Expression, idx: Expression)(val position: InputPosition)
  extends Expression
