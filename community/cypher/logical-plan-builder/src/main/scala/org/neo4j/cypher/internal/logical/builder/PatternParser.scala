/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.builder.PatternParser.Pattern
import org.neo4j.cypher.internal.util.InputPosition.NONE

class PatternParser {
  private val ID = "([a-zA-Z0-9` @_]*)"
  private val REL_TYPES = "([a-zA-Z0-9_|]*)"
  private val regex = s"\\($ID\\)(<?)-\\[?$ID:?$REL_TYPES(\\*?)([0-9]*)(\\.?\\.?)([0-9]*)\\]?-(>?)\\($ID\\)".r
  private var unnamedCount = 0

  private def nextUnnamed(): String = {
    unnamedCount += 1
    "UNNAMED" + unnamedCount
  }

  def parse(pattern: String): Pattern = {
    pattern match {
      case regex(from, incoming, relName, relTypesStr, star, min, dots, max, outgoing, to) =>
        val dir = (incoming, outgoing) match {
          case ("<", "") => SemanticDirection.INCOMING
          case ("", ">") => SemanticDirection.OUTGOING
          case ("", "")  => SemanticDirection.BOTH
          case _         => throw new UnsupportedOperationException(s"Direction $incoming-$outgoing not supported")
        }
        val relTypes =
          if (relTypesStr.isEmpty) Seq.empty
          else relTypesStr.split("\\|").toSeq.map(x => RelTypeName(x)(NONE))
        val length =
          (star, min, dots, max) match {
            case ("", "", "", "")   => SimplePatternLength
            case ("*", "", "", "")  => VarPatternLength(1, None)
            case ("*", x, "..", "") => VarPatternLength(x.toInt, None)
            case ("*", x, "", "")   => VarPatternLength(x.toInt, Some(x.toInt))
            case ("*", "", "..", _) => VarPatternLength(1, Some(max.toInt))
            case ("*", _, "..", _)  => VarPatternLength(min.toInt, Some(max.toInt))
            case _ => throw new UnsupportedOperationException(
                s"$star, $min, $max is not a supported variable length identifier"
              )
          }
        val relNameOrUnnamed =
          if (relName.isEmpty) {
            nextUnnamed()
          } else {
            VariableParser.unescaped(relName)
          }
        Pattern(VariableParser.unescaped(from), dir, relTypes, relNameOrUnnamed, VariableParser.unescaped(to), length)
      case _ => throw new IllegalArgumentException(s"'$pattern' cannot be parsed as a pattern")
    }
  }
}

object PatternParser {

  case class Pattern(
    from: String,
    dir: SemanticDirection,
    relTypes: Seq[RelTypeName],
    relName: String,
    to: String,
    length: PatternLength
  )
}
