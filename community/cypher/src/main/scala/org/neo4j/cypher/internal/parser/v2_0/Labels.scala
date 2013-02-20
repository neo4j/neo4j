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

trait Labels extends Base {
  def labelLit: Parser[Literal] = ":" ~> escapableString ^^ { x => Literal(LabelName(x)) }

  def labelShortForm: Parser[LabelSet] = rep1(labelLit) ^^ {
    case litList =>
      LabelSet(Some(Literal(litList.map(_.v))))
  }

  private def labelExpr: Parser[LabelSet] = expression ^^ (expr => LabelSet(Some(expr)))

  private def labelKeywordForm: Parser[LabelSet] = LABEL ~> (labelShortForm | labelExpr)

  private def labelGroupsForm: Parser[LabelSpec] = rep1sep(labelShortForm, "|") ^^ {
    case labelSets => LabelChoice(labelSets: _*).simplify
  }

  def labelChoiceForm: Parser[LabelSpec] = labelGroupsForm | labelKeywordForm

  def optLabelChoiceForm: Parser[LabelSpec] = opt(labelChoiceForm) ^^ {
    case optSpec => optSpec.getOrElse(LabelSet.empty)
  }

  def labelLongForm: Parser[LabelSet] = labelShortForm | labelKeywordForm

  def optLabelLongForm: Parser[LabelSet] = opt(labelLongForm) ^^ {
    case optSpec => optSpec.getOrElse(LabelSet.empty)
  }

  def expression: Parser[Expression]
}

