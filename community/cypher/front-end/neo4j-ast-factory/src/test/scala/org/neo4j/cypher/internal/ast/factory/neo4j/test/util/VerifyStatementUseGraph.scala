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
package org.neo4j.cypher.internal.ast.factory.neo4j.test.util

import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren

object VerifyStatementUseGraph {

  case class Mismatch(expected: StatementWithGraph, actual: StatementWithGraph) {

    override def toString: String =
      s"""Mismatched use graph
         |Expected: $expected
         |          with graph ${expected.useGraph}
         |Actual  : $actual
         |          with graph ${actual.useGraph}
         |""".stripMargin

  }

  def findUseGraphMismatch(expected: Any, actual: Any): Option[Mismatch] = {
    def commandWithUseGraph(in: Any) = {
      in.folder.treeFold(Seq.empty[(StatementWithGraph, Option[GraphSelection])]) {
        case command: StatementWithGraph => acc => SkipChildren(acc :+ (command -> command.useGraph))
        case _: Statement                => acc => SkipChildren(acc)
        case _                           => acc => TraverseChildren(acc)
      }
    }

    commandWithUseGraph(expected).zip(commandWithUseGraph(actual)).collectFirst {
      case ((expC, expG), (actC, actG)) if expG != actG =>
        Mismatch(expC, actC)
    }
  }
}
