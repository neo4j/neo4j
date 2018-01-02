/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.HasLabel
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException

/**
 * LabelSpec represent parsed label sets before they are turned into either expressions or predicates
 *
 * They come in three forms
 *
 * <ul>
 *   <li>LabelSet.empty denotes that no labels have been parsed</li>
 *   <li>LabelSet(Some(expr)) denotes that a single set of labels has been parsed</li>
 *   <li>LabelChoice(labelSets) denotes that multiple sets of labels have been parsed</li>
 * </ul>
 *
 */
sealed abstract class LabelSpec {
  /**
   * @return true if this has been created without given any labels at all
   */
  def bare: Boolean

  /**
   * @return all LabelSets contained in this LabelSpec
   */
  def allSets: Seq[LabelSet]

  /**
   * @throws SyntaxException if this is a LabelChoice
   * @return this as a LabelSet
   */
  def asLabelSet: LabelSet

  def toPredicates(ident: Identifier): Seq[HasLabel] = asLabelSet.labelVals.map(HasLabel(ident,_))

  /**
   * Reduce a LabelChoice to a LabelSet if possible
   *
   * @return a simplified LabelSpec
   */
  def simplify: LabelSpec = this
}

object LabelSet {
  val empty = LabelSet(Seq.empty)
}

final case class LabelSet(labelVals: Seq[KeyToken]) extends LabelSpec {
  val bare = labelVals.isEmpty
  def allSets = if (bare) Seq.empty else Seq(this)
  def asLabelSet = this
}

final case class LabelChoice(override val allSets: LabelSet*) extends LabelSpec {
  def bare = allSets.isEmpty

  def asLabelSet: LabelSet = throw new SyntaxException("Required single label set or none but found too many")

  override def simplify: LabelSpec =
    if (allSets.isEmpty)
      LabelSet.empty
    else if (allSets.tail.isEmpty)
      allSets.head
    else
      this
}
