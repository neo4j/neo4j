/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands.expressions.{Expression, Literal}
import org.neo4j.cypher.internal.commands.values.LabelName
import org.neo4j.cypher.SyntaxException

trait Labels extends Base {
  def labelLit: Parser[Literal] = ":" ~> identity ^^ { x => Literal(LabelName(x)) }

  def labelShortForm: Parser[LabelSet] = rep1(labelLit) ^^ {
    case litList =>
      LabelSet(Literal(litList.map(_.v)))
  }

  private def labelExpr: Parser[AbstractLabelSet] = expression ^^ (expr => LabelSet(expr))

  private def labelKeywordForm: Parser[AbstractLabelSet] = ignoreCase("label") ~> (labelShortForm | labelExpr)

  private def labelGroupsForm: Parser[LabelSpec] = rep1sep(labelShortForm, "|") ^^ {
    case labelSets => LabelChoice(labelSets: _*).simplify
  }

  def labelChoiceForm: Parser[LabelSpec] = labelGroupsForm | labelKeywordForm

  def optLabelChoiceForm: Parser[LabelSpec] = opt(labelChoiceForm) ^^ {
    case optSpec => optSpec.getOrElse(NoLabels)
  }

  def labelLongForm: Parser[AbstractLabelSet] = labelShortForm | labelKeywordForm

  def optLabelLongForm: Parser[AbstractLabelSet] = opt(labelLongForm) ^^ {
    case optSpec => optSpec.getOrElse(NoLabels)
  }

  def expression: Parser[Expression]
}

sealed abstract class LabelSpec {
  def allSets: Seq[AbstractLabelSet]
  def bare: Boolean

  def asLabelSet: AbstractLabelSet = throw new SyntaxException("Required single label set or none but found too many")
  def simplify: LabelSpec = this

  final def asExpr = asLabelSet.expr
}

sealed abstract class AbstractLabelSet(val expr: Expression) extends LabelSpec {
  def allSets = Seq(this)
  def bare = false

  override def asLabelSet: AbstractLabelSet = this
}

case object NoLabels extends AbstractLabelSet(Literal(Seq.empty)) {
  override def allSets = Seq.empty
  override def bare = true
}

case class LabelSet(override val expr: Expression) extends AbstractLabelSet(expr)

final case class LabelChoice(override val allSets: LabelSet*) extends LabelSpec {
  def bare = allSets.isEmpty

  override def simplify: LabelSpec =
    if (allSets.isEmpty)
      NoLabels
    else if (allSets.tail.isEmpty)
      allSets.head
    else
      this
}
