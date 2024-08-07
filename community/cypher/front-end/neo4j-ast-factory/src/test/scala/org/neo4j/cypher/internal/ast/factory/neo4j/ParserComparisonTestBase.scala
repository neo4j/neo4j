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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.junit.runner.RunWith
import org.neo4j.cypher.internal.ast.HasCatalog
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.scalatest.Assertion
import org.scalatest.Assertions
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import scala.util.Failure
import scala.util.Success
import scala.util.Try

@RunWith(classOf[JUnitRunner])
abstract class ParserComparisonTestBase() extends Assertions with Matchers {
  private val exceptionFactory = new OpenCypherExceptionFactory(None)

  private def fixLineSeparator(message: String): String = {
    // This is needed because current version of scala seems to produce \n from multi line strings
    // This produces a problem with windows line endings \r\n
    if(message.contains(System.lineSeparator()))
      message
    else
      message.replaceAll("\n", System.lineSeparator())
  }

  /**
   * Tests that JavaCC parser produces SyntaxException.
   */
  protected def assertJavaCCException(query: String, expectedMessage: String): Assertion = {
    val exception = the[OpenCypherExceptionFactory.SyntaxException] thrownBy {
      JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    }
    exception.getMessage shouldBe fixLineSeparator(expectedMessage)
  }

  /**
   * Tests that JavaCC parser produces given exception.
   */
  protected def assertJavaCCException(query: String, expected:Exception): Assertion = {
    val exception = the[expected.type] thrownBy {
      JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    }
    exception.getMessage shouldBe fixLineSeparator(expected.getMessage)
  }

  /**
   * Tests that JavaCC parser produces SyntaxException.
   */
  protected def assertJavaCCExceptionStart(query: String, expectedMessage: String): Assertion = {
    val exception = the[OpenCypherExceptionFactory.SyntaxException] thrownBy {
      JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    }
    exception.getMessage should startWith(fixLineSeparator(expectedMessage))
  }

  /**
   * Tests that the JavaCC parser produce correct AST.
   */
  protected def assertJavaCCAST(query: String, expected: Statement): Unit = {
    val ast = JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    ast shouldBe expected
  }

  /**
   * Tests that the parboiled and JavaCC parsers produce the same AST and error positions.
   */
  protected def assertSameAST(query: String, comparePosition: Boolean = true): Unit = assertSameASTForQueries(query, query, comparePosition)

  protected def assertSameASTForQueries(query: String, parBoiledQuery: String, comparePosition: Boolean = true): Unit = {
    withClue(query+System.lineSeparator()) {
      val parboiledParser = new org.neo4j.cypher.internal.parser.CypherParser()
      val parboiledAST = Try(parboiledParser.parse(parBoiledQuery, exceptionFactory))

      val javaccAST = Try(JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator()))

      (javaccAST, parboiledAST) match {
        case (Failure(javaccEx: SyntaxException), Failure(parboiledEx: SyntaxException)) =>
          withClue(Seq(javaccEx, parboiledEx).mkString("", "\n\n", "\n\n")) {
            javaccEx.pos shouldBe parboiledEx.pos
          }
        case (Failure(javaccEx), Success(_)) =>
          throw javaccEx
        case (Success(_), Failure(parboiledEx)) =>
          throw parboiledEx
        case (Success(javaCCStatement), Success(parboiledStatement)) =>
          javaCCStatement shouldBe parboiledStatement
          if (comparePosition) {
            verifyPositions(javaCCStatement, parboiledStatement)
          }
        case (Success(_), Failure(parboiledEx)) =>
          throw parboiledEx
        case _ =>
      }
    }
  }

  def verifyPositions(javaCCAstNode: ASTNode, parboiledASTNode: ASTNode): Unit = {

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
               _: EveryPath |
               _: RelationshipChain |
               _: Yield |
               _: ContainerIndex |
               _: ListSlice |
               _: HasLabelsOrTypes |
               _: SingleQuery |
               _: PeriodicCommitHint |
               _: ReadAdministrationCommand |
               _: HasCatalog |
               _: SetIncludingPropertiesFromMapItem |
               _: SetExactPropertiesFromMapItem => acc => TraverseChildren(acc)
          case returnItems: ReturnItems if returnItems.items.isEmpty => acc => SkipChildren(acc)
          case _: Variable if containsReadAdministratorCommand => acc => TraverseChildren(acc)
          case astNode: ASTNode => acc => TraverseChildren(acc :+ (astNode, astNode.position))
          case _ => acc => TraverseChildren(acc)
        }
      }
    }

    astWithPosition(javaCCAstNode).zip(astWithPosition(parboiledASTNode))
      .foreach {
        case ((astChildNode1, pos1), (astChildNode2, pos2)) => withClue(
          s"ASTNode $astChildNode1 should have the same position as $astChildNode2")(pos1 shouldBe pos2)
        case _ => // Do nothing
      }
  }
}
