/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

trait LabelExpression extends Expression {
  override def toString: String = LabelExpression.stringify(this)
}

object LabelExpression {
  case class Conjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition) extends LabelExpression
  case class Disjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition) extends LabelExpression
  case class Negation(e: LabelExpression)(val position: InputPosition) extends LabelExpression
  case class Label(label: LabelOrRelTypeName)(val position: InputPosition) extends LabelExpression

  private def stringify(labelExpression: LabelExpression): String =
    s"${stringify4(labelExpression)}"

  private def stringify4(labelExpression: LabelExpression): String = labelExpression match {
    case le: Disjunction => s"${stringify3(le.lhs)}|${stringify3(le.rhs)}"
    case le              => s"${stringify3(le)}"
  }

  private def stringify3(labelExpression: LabelExpression): String = labelExpression match {
    case le: Conjunction => s"${stringify2(le.lhs)}&${stringify2(le.rhs)}"
    case le              => s"${stringify2(le)}"
  }

  private def stringify2(labelExpression: LabelExpression): String = labelExpression match {
    case le: Negation => s"!${stringify2(le.e)}"
    case le           => s"${stringify1(le)}"
  }

  private def stringify1(labelExpression: LabelExpression): String = labelExpression match {
    case le: Label    => s"${le.label.name}"
    case le           => s"(${stringify4(le)})"
  }
}

