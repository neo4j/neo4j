/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.compiler.v2_3.ast.IdentifierExpressionTag

object QueryCoder {

  type Mnemonic = Set[QueryTag] => Option[String]

  val orderSpec: Seq[(QueryTag, Mnemonic)] = Seq(
    MatchTag -> "M",
    OptionalMatchTag -> "o",
    RegularMatchTag -> "r",
    ComplexExpressionTag -> "X",
    FilteringExpressionTag -> "f",
    LiteralExpressionTag -> ifPresent(ComplexExpressionTag)("l", "L"),
    ParameterExpressionTag -> ifPresent(ComplexExpressionTag)("p", "P"),
    IdentifierExpressionTag -> ifPresent(IdentifierExpressionTag)("i")
  )

  private val orderIndex: Map[QueryTag, Int] = orderSpec.map(_._1).zipWithIndex.toMap

  private val mnemonicIndex: Map[QueryTag, Mnemonic] = orderSpec.toMap

  def apply(tags: Set[QueryTag]) = {
    // Set(LiteralExpressionTag, MatchTag) =>
    val sortedTags = tags.toSeq.sortBy(orderIndex)
    // => Seq(MatchTag, LiteralExpressionTag) =>
    val mnemonics = sortedTags.map(mnemonicIndex)
    // => Seq(plain("M"), ifPresent(ComplexExpressionTag)(plain("l"), plain("L"))) =>
    val parts = mnemonics.map(_(tags))
    // => Seq(Some("M"), Some("l")) =>
    val result = parts.map(_.getOrElse("")).mkString("")
    // => "Ml" =>
    result
  }

  private def ifPresent(tag: QueryTag)(ifMnemonic: Mnemonic, elseMnemonic: Mnemonic = none): Mnemonic =
    tags => (if (tags(tag)) ifMnemonic else elseMnemonic)(tags)

  private implicit def plain(s: String): Mnemonic = _ => Some(s)

  private object none extends Mnemonic {
    override def apply(ignored: Set[QueryTag]) = None
  }
}
