/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.conditions

import org.neo4j.cypher.internal.apa.v3_4.ASTNode
import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.conditions.noUnnamedPatternElementsInMatch
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

class NoUnnamedPatternElementsInMatchTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = noUnnamedPatternElementsInMatch

  test("unhappy when a node pattern is unnamed") {
    val nodePattern: NodePattern = node(None)
    val ast: ASTNode = SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(chain(chain(node(Some(varFor("n"))), relationship(Some(varFor("p"))), nodePattern), relationship(Some(varFor("r"))), node(Some(varFor("m")))))))_, Seq.empty, None)_,
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, None, None, None)_
    ))_

    condition(ast) shouldBe Seq(s"NodePattern at ${nodePattern.position} is unnamed")
  }

  test("unhappy when a relationship pattern is unnamed") {
    val relationshipPattern: RelationshipPattern = relationship(None)
    val ast: ASTNode = SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(chain(chain(node(Some(varFor("n"))), relationship(Some(varFor("p"))), node(Some(varFor("k")))), relationshipPattern, node(Some(varFor("m")))))))_, Seq.empty, None)_,
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, None, None, None)_
    ))_

    condition(ast) shouldBe Seq(s"RelationshipPattern at ${relationshipPattern.position} is unnamed")
  }

  test("unhappy when there are unnamed node and relationship patterns") {
    val nodePattern: NodePattern = node(None)
    val relationshipPattern: RelationshipPattern = relationship(None)
    val ast: ASTNode = SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(chain(chain(node(Some(varFor("n"))), relationshipPattern, node(Some(varFor("k")))), relationship(Some(varFor("r"))), nodePattern))))_, Seq.empty, None)_,
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, None, None, None)_
    ))_

    condition(ast) shouldBe Seq(s"NodePattern at ${nodePattern.position} is unnamed", s"RelationshipPattern at ${relationshipPattern.position} is unnamed")
  }

  test("happy when all elements in pattern are named") {
    val ast: ASTNode = SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(chain(chain(node(Some(varFor("n"))), relationship(Some(varFor("p"))), node(Some(varFor("k")))), relationship(Some(varFor("r"))), node(Some(varFor("m")))))))_, Seq.empty, None)_,
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, None, None, None)_
    ))_

    condition(ast) shouldBe empty
  }

  test("should leave where clause alone") {
    val where: Where = Where(PatternExpression(RelationshipsPattern(chain(node(None), relationship(None), node(None)))_))_
    val ast: ASTNode = SingleQuery(Seq(
      Match(optional = false, Pattern(Seq(EveryPath(chain(chain(node(Some(varFor("n"))), relationship(Some(varFor("p"))), node(Some(varFor("k")))), relationship(Some(varFor("r"))), node(Some(varFor("m")))))))_, Seq.empty, Some(where))_,
      Return(distinct = false, ReturnItems(includeExisting = false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, None, None, None)_
    ))_

    condition(ast) shouldBe empty
  }

  private def chain(left: PatternElement, rel: RelationshipPattern, right: NodePattern): RelationshipChain = {
    RelationshipChain(left, rel, right)_
  }

  private def relationship(id: Option[Variable]): RelationshipPattern = {
    RelationshipPattern(id, Seq.empty, None, None, SemanticDirection.OUTGOING)_
  }

  private def node(id: Option[Variable]): NodePattern = {
    NodePattern(id, Seq.empty, None)_
  }
}
