/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.parser

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
