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

import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

trait VerifyAstPositionTestSupport extends Assertions with Matchers {

  def verifyPositions(javaCCAstNode: ASTNode, expectedAstNode: ASTNode): Unit = {

    def astWithPosition(astNode: ASTNode) = {
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

    astWithPosition(javaCCAstNode).zip(astWithPosition(expectedAstNode))
      .foreach {
        case ((_, _), (_, InputPosition(a, 0, b))) if a == b => // Ignore DummyPositions
        case ((astChildNode1, pos1), (_, pos2)) =>
          withClue(s"AST node $astChildNode1 was parsed with different positions:") {
            pos1 shouldBe pos2
          }
        case _ => // Do nothing
      }
  }

  implicit protected def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
