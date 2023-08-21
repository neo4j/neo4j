/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
