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

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.Directedness
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.GraphTemplate
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.TemplateId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Represents a 2d rectangular area of a string partitioned by newlines
 *
 * @param lineStart The first line of the area, inclusive
 * @param lineEnd   The last line of the area, inclusive
 * @param colStart  The first column of the area, inclusive
 * @param colEnd    The last column of the area, inclusive
 */
case class InclusiveRect(lineStart: Int, lineEnd: Int, colStart: Int, colEnd: Int) {

  /** All of the points contained within the rect */
  def points: Iterator[Vec2d] =
    for {
      line <- Range.inclusive(lineStart, lineEnd).iterator
      col <- Range.inclusive(colStart, colEnd).iterator
    } yield Vec2d(line, col)

  /** The 'border' around the rect in each direction */
  def surrounding: Iterator[(Vec2d, Direction)] =
    Range.inclusive(lineStart, lineEnd).iterator
      .flatMap(l => Iterator((Vec2d(l, colStart - 1), Direction.Left), (Vec2d(l, colEnd + 1), Direction.Right))) ++
      Range.inclusive(colStart, colEnd).iterator
        .flatMap(c => Iterator((Vec2d(lineStart - 1, c), Direction.Up), (Vec2d(lineEnd + 1, c), Direction.Down)))
}

/** A string split by newlines */
class Lines private (lines: Vector[String]) extends IndexedSeq[String] {
  def apply(i: Int): String = lines(i)

  def length: Int = lines.length

  def get(pos: Vec2d): Option[Char] =
    for {
      line <- lines.lift(pos.line)
      char <- line.lift(pos.col)
    } yield char

  def iterChars: Iterator[(Char, Vec2d)] =
    for {
      (line, lineNum) <- lines.iterator.zipWithIndex
      (char, colNum) <- line.iterator.zipWithIndex
    } yield (char, Vec2d(lineNum, colNum))
}

object Lines {
  def fromStr(str: String) = new Lines(str.linesIterator.toVector)
}

sealed trait Direction {

  def opposite: Direction = this match {
    case Direction.Up    => Direction.Down
    case Direction.Down  => Direction.Up
    case Direction.Left  => Direction.Right
    case Direction.Right => Direction.Left
  }
}

object Direction {
  case object Up extends Direction
  case object Down extends Direction
  case object Left extends Direction
  case object Right extends Direction
}

/** A 2d point in a multiline string */
case class Vec2d(line: Int, col: Int) {
  def +(other: Vec2d): Vec2d = Vec2d(line + other.line, col + other.col)

  def +(dir: Direction): Vec2d = this + Vec2d.fromDir(dir)
}

object Vec2d {

  def fromDir(dir: Direction): Vec2d =
    dir match {
      case Direction.Up    => Vec2d(-1, 0)
      case Direction.Down  => Vec2d(1, 0)
      case Direction.Left  => Vec2d(0, -1)
      case Direction.Right => Vec2d(0, 1)
    }
}

/** A line starting at one point and projecting out in a given direction */
case class Projection(from: Vec2d, direction: Direction) {

  /**
   * Finds the first matching item in the map by following the projection.
   * If a non-space character is hit, or the limit is reached, returns None */
  def find[A](lines: Lines, limit: Int, candidates: Map[Vec2d, A]): Option[A] = {
    var traversingSpace = true
    var i = 0
    var current = from
    var found = Option.empty[A]
    do {
      current = current + direction
      i += 1
      found = candidates.get(current)
      traversingSpace = lines.get(current).forall(_.isWhitespace)
    } while (traversingSpace && i <= limit && found.isEmpty)
    found
  }

}

object Projection {

  def apply(fromLine: Int, fromCol: Int, direction: Direction): Projection =
    Projection(Vec2d(fromLine, fromCol), direction)
}

/** A node representation found in a graph text diagram */
case class ParsedNode(pos: InclusiveRect, name: Option[String], labels: Seq[String])

/** Extracts nodes from graph text diagram */
trait ParsedNodeExtractor {

  def extract(str: String): Iterator[ParsedNode] =
    extract(Lines.fromStr(str))

  def extract(str: Lines): Iterator[ParsedNode]
}

/** A relationship line found in a graph text diagram */
case class ParsedRel(
  points: Set[Vec2d],
  name: Option[String],
  relType: Option[String],
  startProjection: Projection,
  endProjection: Projection,
  direction: SemanticDirection
)

/** Extracts relationships from graph text diagram */
trait ParsedRelExtractor {

  def extract(str: String): Iterator[ParsedRel] =
    extract(Lines.fromStr(str))

  def extract(str: Lines): Iterator[ParsedRel]
}

object GraphTemplateParser {

  /** Parse a 2D ASCII-art diagram of the desired graph topology. For example,
   * {{{
   *           .----[:LIKES]---.
   *           v               |
   *   (a:Person) <-[:LIKES]- (b:Person)
   * }}}
   *
   * A node is denoted by parentheses with an optional alphanumeric name, and optional labels prefixed by a colon.
   *
   * A relationship is denoted by a line drawn with the following characters. It may also optionally contain a name
   * and/or colon-prefixed type in square brackets somewhere along the line.
   *
   * Relationship lines are formed of:
   *
   *   - `-` for horizontal lines
   *   - `|` for vertical lines
   *   - `'` for a bottom corner
   *   - `.` for a top corner
   *
   * One end of a relationship line must terminate in one of the following characters, to indicate its direction.
   *   - `<`
   *   - `>`
   *   - `^`
   *   - `v`
   *
   * @param str the ascii diagram string
   * @param maxGap the maximum permitted gap between the end of a relationship arrow and the node it connects to
   *               (default 1)
   * @return a [[GraphTemplate]] instance
   */
  def parse(str: String, maxGap: Int = 1): GraphTemplate = {
    val template = new GraphTemplate()
    val lines = Lines.fromStr(str)

    val usedPoints = new mutable.HashSet[Vec2d]

    // first look for any nodes
    val nodeAreas = ArrayBuffer.empty[(InclusiveRect, TemplateId)]
    for (parsedNode <- InlineParsedNodeFinder.extract(lines)) {
      val id = template.addNode(parsedNode.name, parsedNode.labels).getId
      usedPoints ++= parsedNode.pos.points
      nodeAreas += (parsedNode.pos -> id)
    }

    // then look for relationships connecting those nodes
    val nodesByPoint = nodeAreas.flatMap { case (rect, id) => rect.points.map(_ -> id) }.toMap
    for (parsedRel <- new MultiLineParsedRelFinder(nodesByPoint.keySet).extract(lines)) {
      for {
        start <- parsedRel.startProjection.find(lines, maxGap, nodesByPoint)
        end <- parsedRel.endProjection.find(lines, maxGap, nodesByPoint)
      } {
        usedPoints ++= parsedRel.points
        parsedRel.direction match {
          case SemanticDirection.OUTGOING =>
            template.addRel(start, end, parsedRel.name, parsedRel.relType, Directedness.Directed)
          case SemanticDirection.INCOMING =>
            template.addRel(end, start, parsedRel.name, parsedRel.relType, Directedness.Directed)
          case SemanticDirection.BOTH =>
            template.addRel(end, start, parsedRel.name, parsedRel.relType, Directedness.Undirected)
        }
      }
    }

    // finally check that there are no unparsed characters in the text
    val unused = lines.iterChars.collect { case (c, p) if c != ' ' && !usedPoints.contains(p) => c }.toSeq
    if (unused.nonEmpty) {
      throw new IllegalArgumentException(
        s"Could not parse graph template diagram; " +
          s"unparsed characters ${unused.mkString("[", ",", "]")}; " +
          s"input template string:\n$str"
      )
    }

    template
  }
}

class GraphTemplateParserTest extends CypherFunSuite {

  test("parse single node") {
    val template = GraphTemplateParser.parse("()")

    template shouldBe new GraphTemplate().addNode()
  }

  test("parse single named node") {
    val template = GraphTemplateParser.parse("(n)")

    template shouldBe new GraphTemplate().addNode("n")
  }

  test("parse single labelled node") {
    val template = GraphTemplateParser.parse("(:Label)")

    template shouldBe new GraphTemplate().addNode(Seq("Label"))
  }

  test("parse single named and labelled node") {
    val template = GraphTemplateParser.parse("(n:Label)")

    template shouldBe new GraphTemplate().addNode("n", "Label")
  }

  test("parse two nodes") {
    val template = GraphTemplateParser.parse("(a) (b)")

    template shouldBe new GraphTemplate().addNode("a").addNode("b")
  }

  test("parse two nodes on different lines") {
    val template = GraphTemplateParser.parse(
      """(a)
        |(b)""".stripMargin
    )

    template shouldBe new GraphTemplate().addNode("a").addNode("b")
  }

  test("parse inline relationship") {
    val template = GraphTemplateParser.parse("(a)->(b)")

    template shouldBe {
      val builder = new GraphTemplate()
      val a = builder.addNode("a").getId
      val b = builder.addNode("b").getId
      builder.addRel(a, b)
      builder
    }
  }

  test("parse vertical relationship") {

    val template = GraphTemplateParser.parse(
      """
        |(a)
        | |
        | v
        |(b)""".stripMargin
    )

    template shouldBe {
      val builder = new GraphTemplate()
      val a = builder.addNode("a").getId
      val b = builder.addNode("b").getId
      builder.addRel(a, b)
      builder
    }
  }

  test("complex diagram 1") {
    val template = GraphTemplateParser.parse(
      """
        |  .--[r:REL]-->(b)
        |(a:A)<----------'
        |""".stripMargin
    )

    template shouldBe {
      val builder = new GraphTemplate()
      val b = builder.addNode("b").getId
      val a = builder.addNode("a", "A").getId
      builder.addRel(a, b, "r", "REL").addRel(b, a)
    }
  }

  test("complex diagram 2") {
    val template = GraphTemplateParser.parse(
      """    .-----------------.
        |    |                 v
        | (n0:S)->(n1)->(n2)->(n3:T)
        |    |
        |    '-->(n4)-->(n5:T)
        |""".stripMargin
    )

    template shouldBe new GraphTemplate()
      .addNode("n0", "S")
      .addNode("n1")
      .addNode("n2")
      .addNode("n3", "T")
      .addNode("n4")
      .addNode("n5", "T")
      .addRel("n0" -> "n3")
      .addRel("n0" -> "n1")
      .addRel("n1" -> "n2")
      .addRel("n2" -> "n3")
      .addRel("n0" -> "n4")
      .addRel("n4" -> "n5")
  }

  test("one whitespace character between relationship arrow and node is permitted") {
    val template = GraphTemplateParser.parse(
      """
        | (a) <-.
        |
        |  '-> (b)
        |""".stripMargin
    )

    template shouldBe new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("b" -> "a")
      .addRel("a" -> "b")
  }

  test("two whitespace characters between relationship arrow and node are rejected") {
    val template =
      """
        | (a)  <-.
        |
        |
        |  '->  (b)
        |""".stripMargin

    an[IllegalArgumentException] shouldBe thrownBy(GraphTemplateParser.parse(template))
  }

  test("two whitespace characters between relationship arrow and node are permitted if setting overridden") {
    val template = GraphTemplateParser.parse(
      """
        | (a)  <-.
        |
        |
        |  '->  (b)
        |""".stripMargin,
      maxGap = 2
    )

    template shouldBe new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("b" -> "a")
      .addRel("a" -> "b")
  }

  test("unused characters are rejected") {
    val template =
      """
        | ()-->()
        |
        | !#
        |
        |""".stripMargin

    the[IllegalArgumentException] thrownBy GraphTemplateParser.parse(template) should have message
      s"Could not parse graph template diagram; unparsed characters [!,#]; input template string:\n$template"
  }

  test("reused node name is rejected") {
    val template = "(n) (n)"

    the[IllegalArgumentException] thrownBy GraphTemplateParser.parse(template) should have message
      "Node with name n already exists in template"
  }

  test("reused relationship name is rejected") {
    val template = "()-[r]->()-[r]->()"

    the[IllegalArgumentException] thrownBy GraphTemplateParser.parse(template) should have message
      "Relationship with name r already exists in template"
  }

  test("relationship line gap can traverse empty part of string") {
    // line 2 has no whitespace in it
    val template =
      """       (a)
        |
        |        |
        |        v
        |       (b)
        |""".stripMargin

    GraphTemplateParser.parse(template) shouldBe new GraphTemplate().addNode("a").addNode("b").addRel("a" -> "b")
  }

  test("relationship cannot have two name sections") {
    val template =
      """
        |()--[:A]--[:B]->()""".stripMargin

    an[IllegalArgumentException] shouldBe thrownBy(GraphTemplateParser.parse(template))
  }

  // miscellaneous invalid templates
  Seq(
    "double-ended relationship" ->
      "()<->()",
    "undirected relationship" ->
      "()--()",
    "relationship arrow going wrong way" ->
      "()-<()",
    "direction arrow not connected correctly" ->
      """ () -v
        |     ()
        |""".stripMargin,
    "unconnected relationship" ->
      """ ()
        |   ->()
        |""".stripMargin,
    "double-ended vertical relationship" ->
      """| ()
         |  ^
         |  |
         |  v
         | ()""".stripMargin
  ).foreach { case (name, template) =>
    test(s"illegal template $name") {
      an[IllegalArgumentException] shouldBe thrownBy(GraphTemplateParser.parse(template))
    }
  }

}
