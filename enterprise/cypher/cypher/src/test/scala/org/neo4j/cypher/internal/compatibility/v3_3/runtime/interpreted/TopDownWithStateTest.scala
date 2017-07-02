/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.TopDownWithState
import org.neo4j.cypher.internal.frontend.v3_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_3.Rewriter
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class TopDownWithStateTest extends CypherFunSuite {
  private def rewriterToTest(): Rewriter = {
    val stateChange: AnyRef => Option[Int] = {
      case NOOP(newState, _) => Some(newState)
      case _ => None
    }

    val rewriteCreator: Int => AnyRef => AnyRef = { (state: Int) =>
      val rewriter: AnyRef => AnyRef = {
        case LIT(v) => LIT(v + state)
        case NOOP(_, src) => src
        case x => x
      }
      rewriter
    }

    TopDownWithState(rewriteCreator, stateChange)
  }

  test("test 1") {
    NOOP(value = 4,
      ADD(
        LIT(1),
        LIT(2))).rewrite(rewriterToTest()) should equal(
      ADD(
        LIT(4 + 1),
        LIT(4 + 2))
    )
  }

  test("test 2") {
    val before =
      NOOP(value = 0,
        ADD(
          NOOP(value = 4,
            ADD(
              LIT(1),
              LIT(2))),
          NOOP(value = 7,
            ADD(
              LIT(1),
              LIT(2)))))

    val expected =
        ADD(
            ADD(
              LIT(4 + 1),
              LIT(4 + 2)),
            ADD(
              LIT(7 + 1),
              LIT(7 + 2)))


    before.rewrite(rewriterToTest()) should equal(expected)
  }
}

trait AST
case class LIT(value: Int) extends AST
case class ADD(lhs: AST, rhs: AST) extends AST
case class NOOP(value: Int, source: AST) extends AST
