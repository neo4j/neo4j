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
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing.MultiLineParsedRelFinder.RelData
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing.MultiLineParsedRelFinder.RelPath
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing.MultiLineParsedRelFinder.chooseOption
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing.MultiLineParsedRelFinder.detailRegex
import org.neo4j.cypher.internal.runtime.spec.graphtemplate.parsing.MultiLineParsedRelFinder.maybeLineChar
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Finds relationships written over (possibly) multiple lines of a 2d string diagram
 */
class MultiLineParsedRelFinder(used: Set[Vec2d]) extends ParsedRelExtractor {

  def extract(lines: Lines): Iterator[ParsedRel] = {

    def tryParseNameType(used: Vec2d => Boolean, pos: Vec2d): Option[RelData] = {
      def findChar(pos: Vec2d, c: Char, dir: Direction): Option[Vec2d] =
        for {
          pos <- Option(pos).filter(p => !used(p))
          value <- lines.get(pos)
          res <- if (value == c) {
            Some(pos)
          } else {
            findChar(pos + dir, c, dir)
          }
        } yield res

      for {
        left <- findChar(pos, '[', Direction.Left)
        right <- findChar(pos, ']', Direction.Right)
        line = lines(pos.line).substring(left.col, right.col + 1)
        dataMatch <- detailRegex.findFirstMatchIn(line)
      } yield {
        val name = Option(dataMatch.group("name")).filter(_.nonEmpty)
        val labels = Option(dataMatch.group("label")).filter(_.nonEmpty)
        RelData(
          InclusiveRect(pos.line, pos.line, left.col, right.col),
          name,
          labels
        )
      }
    }

    def crawlFrom(used: Vec2d => Boolean, c: Char, start: Vec2d): Option[RelPath] = {
      def crawlAlong(path: RelPath): Option[RelPath] = {
        val (_, pos, dir) = path.head
        val nextPos = pos + dir
        if (used(nextPos) || path.contains(nextPos)) {
          Some(path)
        } else {
          val next = for {
            nextChar <- lines.get(nextPos)
            next <- (nextChar, dir) match {
              case ('-', Direction.Left | Direction.Right) | ('|', Direction.Up | Direction.Down) =>
                crawlAlong(path.push(nextChar, nextPos, dir))

              case ('.', Direction.Left | Direction.Right) =>
                crawlAlong(path.push(nextChar, nextPos, Direction.Down))

              case ('\'', Direction.Left | Direction.Right) =>
                crawlAlong(path.push(nextChar, nextPos, Direction.Up))

              case ('.' | '\'', Direction.Up | Direction.Down) =>
                val left = crawlAlong(path.push(nextChar, nextPos, Direction.Left))
                val right = crawlAlong(path.push(nextChar, nextPos, Direction.Right))
                chooseOption(left, right)(RelPath.longest)

              case ('<', Direction.Left) | ('>', Direction.Right) | ('v', Direction.Down) | ('^', Direction.Up) =>
                Some(path.push(nextChar, nextPos, dir))

              case _ => // maybe we reached a label etc - try to parse it
                if (path.data.isEmpty) {
                  tryParseNameType(p => used(p) || path.contains(p), nextPos) match {
                    case Some(data) =>
                      // look around the data rect to pick the line back up
                      data.rect.surrounding
                        .flatMap { case (p, d) =>
                          for {
                            p <- Option(p).filterNot(p => used(p) || path.contains(p))
                            c <- lines.get(p)
                            if ((c, d) match {
                              case ('-', Direction.Left | Direction.Right)
                                | ('|', Direction.Up | Direction.Down) => true
                              case _ => false
                            })
                          } yield (c, p, d)
                        }
                        .toSeq match { // only pick the line back up if there is a single valid re-entry point
                        case Seq((c, p, d)) => crawlAlong(path.withData(data).push(c, p, d))
                        case _              => Some(path)
                      }

                    case None => Some(path)
                  }

                } else {
                  Some(path)
                }
            }
          } yield next

          if (next.isEmpty) {
            Some(path)
          } else next
        }
      }

      c match {
        case '-' =>
          for {
            leftward <- crawlAlong(RelPath.fromStart(c, start, Direction.Left))
            full <- crawlAlong(leftward.reverse)
          } yield full
        case '|' =>
          for {
            upward <- crawlAlong(RelPath.fromStart(c, start, Direction.Up))
            full <- crawlAlong(upward.reverse)
          } yield full
      }
    }

    val usedSoFar = scala.collection.mutable.HashSet.from(used)

    for {
      (c, pos) <- lines.iterChars
      if maybeLineChar(c) && !usedSoFar.contains(pos)
      path <- crawlFrom(x => usedSoFar.contains(x), c, pos)
    } yield {
      usedSoFar ++= path.points
      ParsedRel(
        path.points.toSet,
        path.data.flatMap(_.name),
        path.data.flatMap(_.relType),
        path.startProjection,
        path.endProjection,
        path.semanticDir
      )
    }
  }
}

object MultiLineParsedRelFinder {

  private val detailRegex = """\[\s*(?<name>[\w\d]*)\s*:?\s*(?<label>[\w\d]*)\s*\]""".r

  private def chooseOption[A](a: Option[A], b: Option[A])(choose: (A, A) => A): Option[A] = {
    (a, b) match {
      case (Some(a), Some(b)) => Some(choose(a, b))
      case (a, None)          => a
      case (None, b)          => b
      case _                  => None
    }
  }

  case class RelData(rect: InclusiveRect, name: Option[String], relType: Option[String])

  case class RelPath(segments: Vector[(Char, Vec2d, Direction)], data: Option[RelData]) {
    def head: (Char, Vec2d, Direction) = segments.head
    def withData(data: RelData): RelPath = copy(data = Some(data))
    def push(c: Char, p: Vec2d, d: Direction): RelPath = copy(segments = segments.prepended((c, p, d)))
    def reverse: RelPath = copy(segments = segments.reverse.map { case (c, p, d) => (c, p, d.opposite) })
    def points: Iterator[Vec2d] = segments.iterator.map(_._2) ++ data.iterator.flatMap(_.rect.points)
    def contains(p: Vec2d): Boolean = points.contains(p)
    def length: Int = segments.length

    def startProjection: Projection = {
      val (_, p, d) = segments.last
      Projection(p, d.opposite)
    }

    def endProjection: Projection = {
      val (_, p, d) = segments.head
      Projection(p, d)
    }

    def semanticDir: SemanticDirection =
      (isDirectionIndicator(segments.last._1), isDirectionIndicator(segments.head._1)) match {
        case (true, false)  => SemanticDirection.INCOMING
        case (false, true)  => SemanticDirection.OUTGOING
        case (false, false) => throw new IllegalArgumentException("Line did not have direction indicator")
        case (true, true)   => throw new IllegalArgumentException("Line had two direction indicators")
      }
  }

  object RelPath {

    def fromStart(c: Char, v: Vec2d, d: Direction): RelPath =
      RelPath(Vector((c, v, d)), None)

    def longest(a: RelPath, b: RelPath): RelPath =
      if (a.length > b.length) a else b
  }

  private def maybeLineChar(c: Char): Boolean =
    c match {
      case '-' | '|' => true
      case _         => false
    }

  private def isDirectionIndicator(c: Char): Boolean =
    c match {
      case '>' | '<' | '^' | 'v' => true
      case _                     => false
    }
}

class MultiLineParsedRelFinderTest extends CypherFunSuite {

  test("single rightward line") {
    val rels = find("->")

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(Vec2d(0, 1), Vec2d(0, 0)),
        None,
        None,
        Projection(0, 0, Direction.Left),
        Projection(0, 1, Direction.Right),
        OUTGOING
      )
    )
  }

  test("multiple horizontal lines") {
    val rels = find(
      """ <-
        | -->
        |""".stripMargin
    )

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(Vec2d(0, 2), Vec2d(0, 1)),
        None,
        None,
        Projection(0, 1, Direction.Left),
        Projection(0, 2, Direction.Right),
        INCOMING
      ),
      ParsedRel(
        Set(Vec2d(1, 3), Vec2d(1, 2), Vec2d(1, 1)),
        None,
        None,
        Projection(1, 1, Direction.Left),
        Projection(1, 3, Direction.Right),
        OUTGOING
      )
    )
  }

  test("single upward line") {
    val rels = find(
      """ ^
        | |
        |""".stripMargin
    )

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(Vec2d(1, 1), Vec2d(0, 1)),
        None,
        None,
        Projection(0, 1, Direction.Up),
        Projection(1, 1, Direction.Down),
        INCOMING
      )
    )
  }

  test("multiple vertical lines") {
    val rels = find(
      """ ^ |
        | | v
        |""".stripMargin
    )

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(Vec2d(1, 1), Vec2d(0, 1)),
        None,
        None,
        Projection(0, 1, Direction.Up),
        Projection(1, 1, Direction.Down),
        INCOMING
      ),
      ParsedRel(
        Set(Vec2d(1, 3), Vec2d(0, 3)),
        None,
        None,
        Projection(0, 3, Direction.Up),
        Projection(1, 3, Direction.Down),
        OUTGOING
      )
    )
  }

  test("curved line") {
    val rels = find(
      """
        |   .--. .--.
        | --'  | |  '->
        |      '-'
        |      """.stripMargin
    )

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(
          Vec2d(2, 1),
          Vec2d(1, 5),
          Vec2d(3, 8),
          Vec2d(2, 3),
          Vec2d(3, 6),
          Vec2d(1, 3),
          Vec2d(1, 11),
          Vec2d(3, 7),
          Vec2d(2, 2),
          Vec2d(1, 4),
          Vec2d(1, 6),
          Vec2d(1, 9),
          Vec2d(2, 12),
          Vec2d(2, 11),
          Vec2d(1, 8),
          Vec2d(2, 6),
          Vec2d(2, 8),
          Vec2d(1, 10),
          Vec2d(2, 13)
        ),
        None,
        None,
        Projection(2, 1, Direction.Left),
        Projection(2, 13, Direction.Right),
        OUTGOING
      )
    )
  }

  test("horizontal line with name and type") {
    val rels = find("-[r:Rel]->")

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(
          Vec2d(0, 9),
          Vec2d(0, 2),
          Vec2d(0, 6),
          Vec2d(0, 1),
          Vec2d(0, 8),
          Vec2d(0, 4),
          Vec2d(0, 0),
          Vec2d(0, 3),
          Vec2d(0, 5),
          Vec2d(0, 7)
        ),
        Some("r"),
        Some("Rel"),
        Projection(0, 0, Direction.Left),
        Projection(0, 9, Direction.Right),
        OUTGOING
      )
    )
  }

  test("vertical line with name and type") {
    val rels = find(
      """
        |   |
        | [r:Rel]
        |   |
        |   v
        |""".stripMargin
    )

    rels should contain theSameElementsAs Seq(
      ParsedRel(
        Set(
          Vec2d(2, 1),
          Vec2d(1, 3),
          Vec2d(2, 4),
          Vec2d(2, 2),
          Vec2d(4, 3),
          Vec2d(2, 6),
          Vec2d(2, 3),
          Vec2d(2, 7),
          Vec2d(2, 5),
          Vec2d(3, 3)
        ),
        Some("r"),
        Some("Rel"),
        Projection(1, 3, Direction.Up),
        Projection(4, 3, Direction.Down),
        OUTGOING
      )
    )

  }

  private def find(str: String): Seq[ParsedRel] =
    new MultiLineParsedRelFinder(Set.empty).extract(str).toSeq

}
