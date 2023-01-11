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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.scalacheck.Shrink

import scala.util.Random

/**
 * Not-so-great shrinking for Ast:s
 *
 * Works by rewriting at a random point (lists or options)
 * to create a smaller query
 */
object AstShrinker {

  implicit val shrinkQuery: Shrink[Query] = Shrink[Query] { q =>
    Stream.iterate(shrinkOnce(q))(i => i.flatMap(shrinkOnce))
      .takeWhile(_.isDefined)
      .map(_.get)
      .take(100)
  }

  private def shrinkOnce(q: Query): Option[Query] = {
    val replacer = new RandomReplacer(q)
    replacer.candidates match {
      case 0 => None
      case _ =>
        Some(
          replacer.replace(
            l => List(l.head),
            _ => Option.empty
          )
        )
    }
  }

  private class RandomReplacer[T <: ASTNode](tree: T) {

    val candidates: Int = {
      var seen = 0

      def inc[A](a: A): A = {
        seen += 1
        a
      }

      atCandidates(inc, inc)
      seen
    }

    def replace(
      listReplacement: List[_] => List[_],
      optReplacement: Option[_] => Option[_]
    ): T = {
      var point = Random.nextInt(candidates)

      def origOrReplacement[A](orig: A, replacement: => A): A = point match {
        case 0 => point -= 1; replacement
        case _ => point -= 1; orig
      }

      atCandidates(
        l => origOrReplacement(l, listReplacement(l)),
        o => origOrReplacement(o, optReplacement(o))
      )
    }

    private def atCandidates(
      listCase: List[_] => List[_],
      optCase: Option[_] => Option[_]
    ): T =
      tree.endoRewrite(bottomUp(Rewriter.lift {
        case l: List[_] if l.size > 1    => listCase(l)
        case o: Option[_] if o.isDefined => optCase(o)
      }))
  }

}
