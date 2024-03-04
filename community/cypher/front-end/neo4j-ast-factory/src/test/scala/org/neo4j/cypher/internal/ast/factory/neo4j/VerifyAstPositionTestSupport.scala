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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast._
import org.neo4j.cypher.internal.ast.factory.neo4j.VerifyAstPositionTestSupport.Mismatch
import org.neo4j.cypher.internal.ast.factory.neo4j.VerifyAstPositionTestSupport.findPosMismatch
import org.neo4j.cypher.internal.expressions._
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

trait VerifyAstPositionTestSupport extends Assertions with Matchers {

  def verifyPositions(actual: ASTNode, expected: ASTNode): Unit = {
    findPosMismatch(expected, actual) match {
      case Some(Mismatch(expectedNode, actualNode)) =>
        withClue(s"AST node $actualNode was parsed with different positions:") {
          actualNode.position shouldBe expectedNode.position
        }
      case _ =>
    }
  }
}

object VerifyAstPositionTestSupport {

  case class Mismatch(expected: ASTNode, actual: ASTNode) {

    override def toString: String =
      s"""Mismatched positions
         |Expected: $expected
         |          @ ${expected.position}
         |Actual  : $actual
         |          @ ${actual.position}
         |""".stripMargin
  }

  def findPosMismatch(expected: Any, actual: Any): Option[Mismatch] = {
    def astWithPosition(astNode: Any) = {
      {
        lazy val containsReadAdministratorCommand = astNode.folder.treeExists {
          case _: ReadAdministrationCommand => true
        }

        astNode.folder.treeFold(Seq.empty[(ASTNode, InputPosition)]) {
          case _: Property |
            _: SetPropertyItem |
            _: RemovePropertyItem |
            _: LoadCSV |
            _: UseGraph |
            _: PatternPartWithSelector |
            _: RelationshipChain |
            _: Yield |
            _: ContainerIndex |
            _: ListSlice |
            _: HasLabelsOrTypes |
            _: SingleQuery |
            _: ReadAdministrationCommand |
            _: SetIncludingPropertiesFromMapItem |
            _: SetExactPropertiesFromMapItem => acc => TraverseChildren(acc)
          case returnItems: ReturnItems if returnItems.items.isEmpty => acc => SkipChildren(acc)
          case _: Variable if containsReadAdministratorCommand       => acc => TraverseChildren(acc)
          case astNode: ASTNode => acc => TraverseChildren(acc :+ (astNode -> astNode.position))
          case _                => acc => TraverseChildren(acc)
        }
      }
    }

    astWithPosition(expected).zip(astWithPosition(actual)).collectFirst {
      case ((expNode, p), (actNode, actP)) if p != actP && !(p.offset == p.column && p.line == 0) =>
        Mismatch(expNode, actNode)
    }
  }
}
