/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.parser.ParserTest
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.neo4j.cypher.internal.frontend.v3_4.{PlannerName, ast, parser}
import org.neo4j.cypher.internal.util.v3_4.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.util.v3_4.{CypherException, InputPosition}
import org.parboiled.scala.Rule1

class MultipleGraphClauseSemanticCheckingTest
  extends ParserTest[ast.Statement, SemanticCheckResult]
    with parser.Statement
    with AstConstructionTestSupport {

  // INFO: Use result.dumpAndExit to debug these tests

  implicit val parser: Rule1[Query] = Query

  test("allows COPY OF Node") {
    parsing(
      """MATCH (a:Swedish)
        |CONSTRUCT
        |   NEW (b COPY OF A:Programmer)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("allows COPY OF Node without new var") {
    parsing(
      """MATCH (a:Swedish)
        |CONSTRUCT
        |   NEW (COPY OF A:Programmer)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("fail on copy of with incompatible node type") {
    parsing(
      """MATCH ()-[r]->()
        |CONSTRUCT
        |   NEW (b COPY OF r:Programmer)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set("Type mismatch: r defined with conflicting type Relationship (expected Node)"))
    }
  }

  test("fail on copy of with incompatible rel type") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   NEW ()-[r COPY OF a]->()
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set("Type mismatch: a defined with conflicting type Node (expected Relationship)"))
    }
  }

  test("fail on copy of with incompatible rel type chained") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   NEW ()-[r COPY OF a]->()-[r2:REL]->()
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set("Type mismatch: a defined with conflicting type Node (expected Relationship)"))
    }
  }

  test("Do not require type for cloned relationships") {
    parsing(
      """MATCH (a)-[r]-(b)
        |CONSTRUCT
        |   CLONE a, r, b
        |   NEW (a)-[r]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("Do not require type for implicitly cloned relationships") {
    parsing(
      """MATCH (a)-[r]-(b)
        |CONSTRUCT
        |NEW (a)-[r]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("Do not require type for aliased cloned relationships") {
    parsing(
      """MATCH (a)-[r]-(b)
        |CONSTRUCT
        |CLONE r as newR
        |NEW (a)-[newR]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  test("Require type for new relationships") {
    parsing(
      """MATCH (a), (b)
        |CONSTRUCT
        |   CLONE a, b
        |   NEW (a)-[r]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Exactly one relationship type must be specified for NEW. Did you forget to prefix your relationship type with a ':'?")
      )
    }
  }


  test("Require type for new relationships chained") {
    parsing(
      """MATCH (a), (b)
        |CONSTRUCT
        |   CLONE a, b
        |   NEW (a)-[r]->(b)-[r2:REL]->(c)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Exactly one relationship type must be specified for NEW. Did you forget to prefix your relationship type with a ':'?")
      )
    }
  }

  test("Allow multiple usages of a newly created nodes for connections") {
    parsing(
      """MATCH (a), (b)
        |CONSTRUCT
        |   NEW (a2)
        |   NEW (a2)-[r1:REL]->(b)
        |   NEW (a2)-[r2:REL]->(a)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errors shouldBe empty
    }
  }

  // TODO: Fix scoping of registered variables
  ignore("Do not allow multiple usages of a newly created node") {
    parsing(
      """MATCH (a), (b)
        |CONSTRUCT
        |   NEW (a2)
        |   NEW (a2)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Variable `a2` already declared")
      )
    }
  }

  test("Do not allow multiple usages of a newly created relationship in NEW") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   CLONE a, b
        |   NEW (a)-[r2:REL]->(b)
        |   NEW (b)-[r2:REL]->(a)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Relationship `r2` can only be declared once")
      )
    }
  }

  test("Do not allow to specify conflicting base nodes for new nodes") {
    parsing(
      """MATCH (a),(b)
        |CONSTRUCT
        |   NEW (a2 COPY OF a)
        |   NEW (a2 COPY OF b)-[r:REL]->(a)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Node a2 cannot inherit from multiple bases a, b")
      )
    }
  }

  test("Do not allow manipulating labels of cloned aliased nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   CLONE a as newA
        |   NEW (newA:FOO)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating labels of cloned nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   CLONE a
        |   NEW (a:FOO)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating labels of implicitly cloned nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   NEW (a:FOO)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating properties of cloned aliased nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   CLONE a as newA
        |   NEW (newA {foo: "bar"})
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating properties of cloned nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   CLONE a
        |   NEW (a {foo: "bar"})
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating properties of implicitly cloned nodes") {
    parsing(
      """MATCH (a)
        |CONSTRUCT
        |   NEW (a {foo: "bar"})
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned node is not allowed. Use COPY OF to manipulate the node")
      )
    }
  }

  test("Do not allow manipulating types of cloned aliased relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   CLONE a, r as newR, b
        |   NEW (a)-[newR:FOO]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  test("Do not allow manipulating types of cloned relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   CLONE a, r, b
        |   NEW (a)-[r:FOO]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  test("Do not allow manipulating types of implicitly cloned relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   NEW (a)-[r:FOO]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  test("Do not allow manipulating properties of cloned aliased relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   CLONE a, r as newR, b
        |   NEW (a)-[newR {foo: "bar"}]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  test("Do not allow manipulating properties of cloned relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   CLONE a, r, b
        |   NEW (a)-[r {foo: "bar"}]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  test("Do not allow manipulating properties of implicitly cloned relationships") {
    parsing(
      """MATCH (a)-[r]->(b)
        |CONSTRUCT
        |   NEW (a)-[r {foo: "bar"}]->(b)
        |RETURN GRAPH""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(
        Set("Modification of a cloned relationship is not allowed. Use COPY OF to manipulate the relationship")
      )
    }
  }

  override def convert(astNode: ast.Statement): SemanticCheckResult = {
    val rewritten = PreparatoryRewriting.transform(TestState(Some(astNode)), TestContext).statement()
    val initialState = SemanticState.clean.withFeatures(SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
    rewritten.semanticCheck(initialState)
  }

  implicit final class RichSemanticCheckResult(val result: SemanticCheckResult) {
    def state: SemanticState = result.state

    def errors: Seq[SemanticErrorDef] = result.errors

    def errorMessages: Set[String] = errors.map(_.msg).toSet
  }

  //noinspection TypeAnnotation
  case class TestState(override val maybeStatement: Option[ast.Statement]) extends BaseState {
    override def queryText: String = statement.toString

    override def startPosition: None.type = None

    override object plannerName extends PlannerName {
      override def name: String = "Test"

      override def version: String = "3.4"

      override def toTextOutput: String = name
    }


    override def maybeSemantics = None

    override def maybeExtractedParams = None

    override def maybeSemanticTable = None

    override def accumulatedConditions = Set.empty

    override def withStatement(s: Statement) = copy(Some(s))

    override def withSemanticTable(s: SemanticTable) = ???

    override def withSemanticState(s: SemanticState) = ???

    override def withParams(p: Map[String, Any]) = ???

    override def initialFields: Map[String, CypherType] = Map.empty
  }

  //noinspection TypeAnnotation
  object TestContext extends BaseContext {
    override def tracer = CompilationPhaseTracer.NO_TRACING

    override def notificationLogger = mock[InternalNotificationLogger]

    override object exceptionCreator extends ((String, InputPosition) => CypherException) {
      override def apply(msg: String, pos: InputPosition): CypherException = throw new CypherException() {
        override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
          mapper.internalException(msg, this)
      }
    }

    override def monitors = mock[Monitors]

    override def errorHandler = _ => ()
  }

}