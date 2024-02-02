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
package org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Finds nodes described on a single line
 */
object InlineParsedNodeFinder extends ParsedNodeExtractor {
  private val simpleNodeRegex = """\(\s*(?<name>[\w\d]*)\s*:?\s*(?<label>[\w\d]*)\s*\)""".r

  def extract(lines: Lines): Iterator[ParsedNode] = {
    for {
      (str, line) <- lines.iterator.zipWithIndex
      nodeMatch <- simpleNodeRegex.findAllMatchIn(str)
    } yield {
      val name = Option(nodeMatch.group("name")).filter(_.nonEmpty)
      val labels = Seq(nodeMatch.group("label")).filter(_.nonEmpty)
      ParsedNode(
        InclusiveRect(line, line, nodeMatch.start, nodeMatch.end - 1),
        name,
        labels
      )
    }
  }
}

class InlineParsedNodeFinderTest extends CypherFunSuite {

  test("Find node in single line") {
    val res = positions("()")

    res shouldBe Seq(InclusiveRect(0, 0, 0, 1))
  }

  test("Find named and labelled node in single line") {
    val res = positions("(n:L)")

    res shouldBe Seq(InclusiveRect(0, 0, 0, 4))
  }

  test("Find two nodes in single line") {
    val res = positions("() ()")

    res shouldBe Seq(InclusiveRect(0, 0, 0, 1), InclusiveRect(0, 0, 3, 4))
  }

  test("Find nodes on multiple lines") {
    val res = positions(
      """()
        |  ()
        | ()""".stripMargin
    )

    res shouldBe Seq(
      InclusiveRect(0, 0, 0, 1),
      InclusiveRect(1, 1, 2, 3),
      InclusiveRect(2, 2, 1, 2)
    )
  }

  private def positions(str: String): Seq[InclusiveRect] = InlineParsedNodeFinder.extract(str).map(_.pos).toSeq
}
