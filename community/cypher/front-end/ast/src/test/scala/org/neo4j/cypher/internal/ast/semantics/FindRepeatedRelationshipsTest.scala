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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.patternForMatch
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class FindRepeatedRelationshipsTest extends CypherFunSuite {

  private val pos = DummyPosition(0)
  private val pos2 = DummyPosition(1)
  private val node = NodePattern(None, None, None, None)(pos)
  private val relR = Variable("r")(pos)
  private val relRCopy = Variable("r")(pos2)
  private val relS = Variable("s")(pos)

  test("does find repeated relationships across pattern parts") {
    val relPath0 = RelationshipChain(node, relPattern(relR), node)(pos)
    val relPath1 = RelationshipChain(node, relPattern(relRCopy), node)(pos)
    val pattern = patternForMatch(relPath0, relPath1)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq(relR))
    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false)(0).position should equal(relR.position)
  }

  test("does not find repeated relationships across pattern parts in variable length mode") {
    val relPath0 = RelationshipChain(node, relPattern(relR), node)(pos)
    val relPath1 = RelationshipChain(node, relPattern(relRCopy), node)(pos)
    val pattern = patternForMatch(relPath0, relPath1)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq.empty)
  }

  test("does find repeated variable length relationships across pattern parts") {
    val relPath0 = RelationshipChain(node, variableLengthRelPattern(relR), node)(pos)
    val relPath1 = RelationshipChain(node, variableLengthRelPattern(relRCopy), node)(pos)
    val pattern = patternForMatch(relPath0, relPath1)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq(relR))
    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true)(0).position should equal(
      relR.position
    )
  }

  test("does not find repeated variable length relationships across pattern parts without variable length mode") {
    val relPath0 = RelationshipChain(node, variableLengthRelPattern(relR), node)(pos)
    val relPath1 = RelationshipChain(node, variableLengthRelPattern(relRCopy), node)(pos)
    val pattern = patternForMatch(relPath0, relPath1)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq.empty)
  }

  test("does find repeated relationships in a long rel chain") {
    val relPath = relChain(relR, relS, relRCopy)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq(relR))
    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false)(0).position should equal(relR.position)
  }

  test("does not find repeated relationships in a long rel chain in variable length mode") {
    val relPath = relChain(relR, relS, relRCopy)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq.empty)
  }

  test("does find repeated variable length relationships in a long rel chain") {
    val relPath = variableLengthRelChain(relR, relS, relRCopy)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq(relR))
    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true)(0).position should equal(
      relR.position
    )
  }

  test("does not find repeated variable length relationships in a long rel chain without variable length mode") {
    val relPath = variableLengthRelChain(relR, relS, relRCopy)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq.empty)
  }

  test("does not find repeated relationships across pattern parts if there is none") {
    val relPath = expressions.RelationshipChain(node, relPattern(relR), node)(pos)
    val otherRelPath = expressions.RelationshipChain(node, relPattern(relS), node)(pos)
    val pattern = patternForMatch(relPath, otherRelPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq.empty)
  }

  test("does not find repeated variable length relationships across pattern parts if there is none") {
    val relPath = expressions.RelationshipChain(node, variableLengthRelPattern(relR), node)(pos)
    val otherRelPath = expressions.RelationshipChain(node, variableLengthRelPattern(relS), node)(pos)
    val pattern = patternForMatch(relPath, otherRelPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq.empty)
  }

  test("does not find repeated relationships in a long rel chain if there is none") {
    val relPath = relChain(relS, relR)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = false) should equal(Seq.empty)
  }

  test("does not find repeated variable length relationships in a long rel chain if there is none") {
    val relPath = variableLengthRelChain(relS, relR)
    val pattern = patternForMatch(relPath)

    SemanticPatternCheck.findRepeatedRelationships(pattern, varLength = true) should equal(Seq.empty)
  }

  private def relChain(ids: Variable*) =
    ids.foldLeft(node.asInstanceOf[SimplePattern]) {
      (n, id) => expressions.RelationshipChain(n, relPattern(id), node)(pos)
    }

  private def variableLengthRelChain(ids: Variable*) =
    ids.foldLeft(node.asInstanceOf[SimplePattern]) {
      (n, id) => expressions.RelationshipChain(n, variableLengthRelPattern(id), node)(pos)
    }

  private def relPattern(id: Variable) =
    RelationshipPattern(Some(id), None, None, None, None, SemanticDirection.OUTGOING)(pos)

  private def variableLengthRelPattern(id: Variable) = {
    val range = expressions.Range(Some(UnsignedDecimalIntegerLiteral("2")(pos)), None)(pos)
    RelationshipPattern(Some(id), None, Some(Some(range)), None, None, SemanticDirection.OUTGOING)(pos)
  }
}
