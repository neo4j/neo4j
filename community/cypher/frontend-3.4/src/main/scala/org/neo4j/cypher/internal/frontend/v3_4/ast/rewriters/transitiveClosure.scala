/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.Where
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, topDown}
import org.neo4j.cypher.internal.v3_4.expressions._

import scala.annotation.tailrec

/**
  * Transitive closure of where clauses.
  *
  * Given a where clause, `WHERE a.prop = b.prop AND b.prop = 42` we rewrite the query
  * into `WHERE a.prop = 42 AND b.prop = 42`
  */
case object transitiveClosure extends StatementRewriter {

  override def description: String = "transitive closure in where clauses"

  override def instance(ignored: BaseContext): Rewriter = transitiveClosureRewriter

  override def postConditions: Set[Condition] = Set.empty

  private case object transitiveClosureRewriter extends Rewriter {

    def apply(that: AnyRef): AnyRef = instance.apply(that)

    private val instance: Rewriter = topDown(Rewriter.lift {
      case where@Where(expression) =>
        recurse(where, w => w.endoRewrite(whereRewriter))
    })

    @tailrec
    private def recurse(in: Where, f: Where => Where): Where = {
      val out = f(in)
      if (in == out) out
      else recurse(out, f)
    }

    //Collects property equalities, e.g `a.prop = 42`
    private def collect(e: Expression): Map[Property, Expression] = e.treeFold(Map.empty[Property, Expression]) {
      case _: Or => (acc) => (acc, None)
      case _: And => (acc) => (acc, Some(identity))
      case Equals(_: Property, _: Property) => (acc) => (acc, None)
      case Equals(p: Property, other) => (acc) => (acc + (p -> other), None)
    }

    private val whereRewriter: Rewriter = topDown(Rewriter.lift {
      case and@And(lhs, rhs) =>
        val inner = andRewriter(collect(lhs) ++ collect(rhs))
        and.copy(lhs = lhs.endoRewrite(inner), rhs = rhs.endoRewrite(inner))(and.position)
    })

    private def andRewriter(map: Map[Property, Expression]): Rewriter = topDown(Rewriter.lift {
      case equals@Equals(p1: Property, p2: Property) if map.contains(p2) =>
        equals.copy(rhs = map(p2))(equals.position)
    })
  }

}
