/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser.javacc

import scala.collection.JavaConverters

object ParseExceptions extends RuntimeException {

  def expected(expectedTokenSequences: Array[Array[Int]], tokenImage: Array[String]): java.util.List[String] = {
    JavaConverters.seqAsJavaList(processExpectedList(expectedTokenSequences.toSeq.flatten, tokenImage))
  }

  def processExpectedList(expected: Seq[Int], tokenImage: Array[String]): Seq[String] = {
    if (expected.contains(CypherConstants.IDENTIFIER)) {
      if (expected.contains(CypherConstants.PLUS)) {
        filterExpression(expected)
          .map(tokenImage(_))
          .filter(!_.equals("\"$\"")) :+ "an expression"
      } else {
        filterIdentifierTokens(expected)
          .map(token =>
            if (token.equals(CypherConstants.IDENTIFIER)) {
              "an identifier"
            } else {
              val image = tokenImage(token)
              if (image.equals("\"$\"")) {
                "a parameter"
              } else {
                image
              }
            })
      }
    } else {
      expected.map(tokenImage(_)).distinct
    }
  }

  def filterExpression(expected: Seq[Int]): Seq[Int] = {
    filterIdentifierTokens(expected).groupBy(identity).mapValues(_.size)
      .map({ case (token, count) =>
        if (ExpressionTokens.tokens.contains(token)) {
          (token, count - 1)
        } else {
          (token, count)
        }
      }).filter({ case (_, count) => count > 0 })
      .keySet.toSeq
  }

  def filterIdentifierTokens(expected: Seq[Int]): Seq[Int] = {
    expected.groupBy(identity).mapValues(_.size).map({ case (token, count) =>
      if (IdentifierTokens.tokens.contains(token)) {
        (token, count - 1)
      } else {
        (token, count)
      }
    }).filter({ case (_, count) => count > 0 })
      .keySet.toSeq
  }
}
