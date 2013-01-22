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
import org.neo4j.cypher.internal.commands.values.LabelValue

trait LabelParsing {
  self: Base =>

  def optLabelSeq = opt(labelSeq) ^^ {
    case Some(labels) => labels
    case _            => Literal(Seq.empty)
  }

  def labelSeq = longLabelSeq | shortLabelSeq

  def longLabelSeq: Parser[Expression]  = ignoreCase("LABEL") ~> labelLitSeq
  def shortLabelSeq: Parser[Literal]    = labelLitSeq

  def labelLitSeq = rep1(labelLit) ^^ { (x: List[Literal]) =>
    Literal(x map { _.v.asInstanceOf[LabelValue] } )
  }
}