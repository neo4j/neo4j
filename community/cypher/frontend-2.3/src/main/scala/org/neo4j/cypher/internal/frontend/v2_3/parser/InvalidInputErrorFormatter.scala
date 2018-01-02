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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.parboiled.common.StringUtils
import org.parboiled.errors.{DefaultInvalidInputErrorFormatter, InvalidInputError}
import org.parboiled.matchers.{Matcher, TestNotMatcher}
import org.parboiled.support.{Chars, MatcherPath}

import scala.collection.JavaConversions._

class InvalidInputErrorFormatter extends DefaultInvalidInputErrorFormatter {

  override def format(error: InvalidInputError) : String = {
    if (error == null)
      ""
    else {
      val len = error.getEndIndex - error.getStartIndex
      val sb = new StringBuilder()
      if (len > 0) {
        val char = error.getInputBuffer.charAt(error.getStartIndex)
        if (char == Chars.EOI) {
          sb.append("Unexpected end of input")
        } else {
          sb.append("Invalid input '").append(StringUtils.escape(String.valueOf(char)))
          if (len > 1) sb.append("...")
          sb.append('\'')
        }
      } else {
        sb.append("Invalid input")
      }
      val expectedString = getExpectedString(error)
      if (StringUtils.isNotEmpty(expectedString)) {
        sb.append(": expected ").append(expectedString)
      }
      sb.toString()
    }
  }

  override def getExpectedString(error: InvalidInputError) : String = {
    val pathStartIndex = error.getStartIndex - error.getIndexDelta

    val labels = error.getFailedMatchers.toList.flatMap(path => {
      val labelMatcher = findProperLabelMatcher(path, pathStartIndex)
      if (labelMatcher == null) {
        List()
      } else {
        getLabels(labelMatcher).filter(_ != null).flatMap(_.trim match {
          case l@"','" => Seq(l)
          case "" => Seq()
          case l => l.split(",").map(_.trim)
        })
      }
    }).distinct

    join(labels)
  }

  private def findProperLabelMatcher(path: MatcherPath, errorIndex: Int) : Matcher = {
    val elements = unfoldRight(path) { p => if (p == null) None else Some(p.element -> p.parent) }.reverse

    val matcher = for (element <- elements.takeWhile(!_.matcher.isInstanceOf[TestNotMatcher]).find(e => {
      e.startIndex == errorIndex && e.matcher.hasCustomLabel
    })) yield element.matcher

    matcher.orNull
  }

  private def unfoldRight[A, B](seed: B)(f: B => Option[(A, B)]): List[A] = f(seed) match {
    case Some((a, b)) => a :: unfoldRight(b)(f)
    case None => Nil
  }
}
