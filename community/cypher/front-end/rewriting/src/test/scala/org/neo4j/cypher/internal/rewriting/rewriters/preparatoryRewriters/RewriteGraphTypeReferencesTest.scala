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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.reset
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AstGraphTypeConstructionTestSupport
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.prettifier.GraphTypeStringifier
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase
import org.neo4j.cypher.internal.rewriting.conditions.NoInlineConstraints
import org.neo4j.cypher.internal.rewriting.conditions.NoReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.util.CancellationChecker.NeverCancelled
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObject

import scala.collection.immutable.ArraySeq

class RewriteGraphTypeReferencesTest extends CypherFunSuite with AstGraphTypeConstructionTestSupport {

  val mockExceptionFactory: CypherExceptionFactory = mock[CypherExceptionFactory]
  val rewriter: Rewriter = RewriteGraphTypeReferences.getRewriter(mockExceptionFactory)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    reset(mockExceptionFactory)
  }

  test("rewrite node type references in edge") {
    val gt = alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Person",
        "p",
        propertyType("name", StringType(isNullable = true))
      ),
      edgeType(
        nodeTypeRefByVar("p"),
        "KNOWS",
        nodeTypeRefByLabel("Person"),
        propertyType("since", DateType(isNullable = true))
      )
    ))
    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Person",
        "p",
        propertyType("name", StringType(isNullable = true))
      ),
      edgeType(
        identifyingNodeTypeRef("Person", "p"),
        "KNOWS",
        identifyingNodeTypeRef("Person", "p"),
        propertyType("since", DateType(isNullable = true))
      )
    ))
  }

  test("do not rewrite empty node type references in edge") {
    val gt = alterCurrentGraphTypeSet(graphType(
      edgeType(
        EmptyNodeTypeReference()(pos),
        "KNOWS",
        EmptyNodeTypeReference()(pos),
        propertyType("since", DateType(isNullable = true))
      )
    ))
    rewriter.apply(gt) shouldBe gt
  }

  test("do not rewrite identifying node type references in edge") {
    val gt = alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Person",
        "p",
        propertyType("name", StringType(isNullable = true))
      ),
      edgeType(
        identifyingNodeTypeRef("Person"),
        "KNOWS",
        identifyingNodeTypeRef("Person"),
        propertyType("since", DateType(isNullable = true))
      )
    ))
    rewriter.apply(gt) shouldBe gt
  }

  test("do not rewrite node label references in edge") {
    val gt = alterCurrentGraphTypeSet(graphType(
      edgeType(
        EmptyNodeTypeReference()(pos),
        "KNOWS",
        nodeTypeRefByLabel("Person"),
        propertyType("since", DateType(isNullable = true))
      )
    ))
    rewriter.apply(gt) shouldBe gt
  }

  test("rewrite node type references constraint") {
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        )
      ),
      Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name", defaultPos))))
    ))

    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        )
      ),
      Seq(keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name", defaultPos))))
    ))
  }

  test("rewrite edge type references constraint by variable") {
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          "et",
          nodeTypeRefByLabel("Person"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(keyConstraint(edgeTypeRefByVar("et"), ArraySeq(prop("et", "name", defaultPos))))
    ))
    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          identifyingNodeTypeRef("Person", "p"),
          "KNOWS",
          "et",
          identifyingNodeTypeRef("Person", "p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(keyConstraint(identifyingEdgeTypeRef("KNOWS", "et"), ArraySeq(prop("et", "name", defaultPos))))
    ))
  }

  test("rewrite edge type references constraint by label") {
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          "et",
          nodeTypeRefByLabel("Person"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(keyConstraint(edgeTypeRefByLabel("KNOWS", "p"), ArraySeq(prop("p", "name", defaultPos))))
    ))
    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          identifyingNodeTypeRef("Person", "p"),
          "KNOWS",
          "et",
          identifyingNodeTypeRef("Person", "p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(keyConstraint(identifyingEdgeTypeRef("KNOWS", "p"), ArraySeq(prop("p", "name", defaultPos))))
    ))
  }

  test("rewrite inline property type constraint") {
    val gt = alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Person",
        "p",
        propertyType(
          "name",
          StringType(isNullable = true),
          PropertyInlineKeyConstraint()(defaultPos)
        )
      ),
      edgeType(
        nodeTypeRefByVar("p"),
        "KNOWS",
        "k",
        nodeTypeRefByLabel("Person"),
        propertyType(
          "since",
          DateType(isNullable = true),
          PropertyInlineUniquenessConstraint()(defaultPos)
        )
      )
    ))
    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          identifyingNodeTypeRef("Person", "p"),
          "KNOWS",
          "k",
          identifyingNodeTypeRef("Person", "p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(
        keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))),
        uniquenessConstraint(identifyingEdgeTypeRef("KNOWS", "k"), ArraySeq(prop("k", "since")))
      )
    ))
  }

  test("rewrite inline element type constraint") {
    val gt = alterCurrentGraphTypeSet(graphType(
      nodeTypeWithConstraints(
        "Person",
        "p",
        Set(KeyConstraint(ArraySeq(prop("p", "name")))(pos)),
        propertyType(
          "name",
          StringType(isNullable = true)
        )
      ),
      edgeTypeWithConstraints(
        nodeTypeRefByVar("p"),
        "KNOWS",
        "k",
        nodeTypeRefByLabel("Person"),
        Set(UniquenessConstraint(ArraySeq(prop("k", "since")))(pos)),
        propertyType(
          "since",
          DateType(isNullable = true)
        )
      )
    ))
    rewriter.apply(gt) shouldBe alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          identifyingNodeTypeRef("Person", "p"),
          "KNOWS",
          "k",
          identifyingNodeTypeRef("Person", "p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      Seq(
        keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))),
        uniquenessConstraint(identifyingEdgeTypeRef("KNOWS", "k"), ArraySeq(prop("k", "since")))
      )
    ))
  }

  test("Rewrite RE-1") {
    val re1 = alterCurrentGraphTypeSet(graphType(
      Seq(
        nodeTypeWithLabelsAndConstraints(
          "Student",
          "s",
          Set("Person"),
          Set((
            UniquenessConstraint(ArraySeq(prop(varFor("s"), "name"), prop(varFor("s"), "birthday")))(defaultPos),
            OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
          )),
          propertyType("name", StringType(isNullable = false)),
          propertyType("studId", IntegerType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("birthday", DateType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Location"),
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        nodeType("Site", Set("Location"), propertyType("name", StringType(isNullable = true))),
        edgeType(nodeTypeRefByVar("s"), "LIVES_IN", identifyingNodeTypeRef("City")),
        edgeType(nodeTypeRefByVar("s"), "VISITED", nodeTypeRefByLabel("Location"))
      ),
      Seq(
        uniquenessConstraint(edgeTypeRefByLabel("LegacyRel", "x"), ArraySeq(prop(varFor("x"), "foo"))),
        keyConstraint(
          "mySiteConstraint",
          identifyingNodeTypeRef("Site", "st"),
          ArraySeq(prop(varFor("st"), "name"))
        ),
        propertyTypeConstraint(
          nodeTypeRefByLabel("Person", "p"),
          ArraySeq(prop(varFor("p"), "age")),
          IntegerType(isNullable = true)(defaultPos)
        )
      )
    ))
    val rewritten: AlterCurrentGraphType =
      rewriter.apply(re1).asInstanceOf[Statements].statements.head.asInstanceOf[AlterCurrentGraphType]
    val expected = graphType(
      Seq(
        nodeType(
          "Student",
          "s",
          Set("Person"),
          propertyType("name", StringType(isNullable = false)),
          propertyType("studId", IntegerType(isNullable = true)),
          propertyType("birthday", DateType(isNullable = true))
        ),
        nodeType("City", Set("Location"), propertyType("name", StringType(isNullable = true))),
        nodeType("Site", Set("Location"), propertyType("name", StringType(isNullable = true))),
        edgeType(identifyingNodeTypeRef("Student", "s"), "LIVES_IN", identifyingNodeTypeRef("City")),
        edgeType(identifyingNodeTypeRef("Student", "s"), "VISITED", nodeTypeRefByLabel("Location"))
      ),
      Seq(
        uniquenessConstraint(edgeTypeRefByLabel("LegacyRel", "x"), ArraySeq(prop(varFor("x"), "foo"))),
        keyConstraint("mySiteConstraint", identifyingNodeTypeRef("Site", "st"), ArraySeq(prop(varFor("st"), "name"))),
        propertyTypeConstraint(
          nodeTypeRefByLabel("Person", "p"),
          ArraySeq(prop(varFor("p"), "age")),
          IntegerType(isNullable = true)(defaultPos)
        ),
        keyConstraint(identifyingNodeTypeRef("Student", "s"), ArraySeq(prop(varFor("s"), "studId"))),
        uniquenessConstraint(
          identifyingNodeTypeRef("Student", "s"),
          ArraySeq(prop(varFor("s"), "name"), prop(varFor("s"), "birthday")),
          OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
        ),
        keyConstraint(identifyingNodeTypeRef("City", "n"), ArraySeq(prop(varFor("n"), "name")))
      )
    )
    withClue(
      s"Rewritten:\n${GraphTypeStringifier(rewritten.graphType)}\nExpected:\n${GraphTypeStringifier(expected)}"
    ) {
      rewritten shouldBe alterCurrentGraphTypeSet(expected).statements.head.asInstanceOf[AlterCurrentGraphType]
    }
  }

  test("do not rewrite constraint names") {
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(),
      Seq(
        constraintName("constraint1"),
        constraintName("constraint2")
      )
    ))
    rewriter.apply(gt) shouldBe gt
  }

  test("Rewrite invalid inline constraint should generate invalid constraint") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(nodeTypeWithConstraints(
      "Person",
      "p",
      Set(UniquenessConstraint(ArraySeq(prop(varFor("s"), "name")))(defaultPos)),
      propertyType(
        "name",
        StringType(isNullable = true)
      )
    )))

    // When
    rewriter.apply(gt) shouldBe (
      alterCurrentGraphTypeSet(graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType(
              "name",
              StringType(isNullable = true)
            )
          )
        ),
        Seq(
          uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop(varFor("s"), "name")))
        )
      ))
    )
  }

  test("Rewrite endpoints should not duplicate variables") {
    val gt = alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Node",
        "n",
        propertyType(
          "name",
          AnyType(isNullable = false)
        )
      ),
      edgeType(nodeTypeRefByVar("n"), "REL", nodeTypeRefByLabel("Node"))
    ))

    // When
    val rewritten = rewriter.apply(gt)

    NoReferenceEqualityAmongVariables.apply(rewritten)(NeverCancelled) shouldBe empty
    rewritten shouldBe alterCurrentGraphTypeSet(graphType(
      nodeType(
        "Node",
        "n",
        propertyType(
          "name",
          AnyType(isNullable = false)
        )
      ),
      edgeType(identifyingNodeTypeRef("Node", "n"), "REL", identifyingNodeTypeRef("Node", "n"))
    ))
  }

  // Negative
  test("Rewrite invalid edge type should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      edgeType(nodeTypeRefByVar("p"), "EDGE", EmptyNodeTypeReference()(pos))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("graph type element referenced by 'p' not found"),
      any()
    )
  }

  test("Rewrite invalid identifying node reference in constraint should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(),
      Seq(keyConstraint(identifyingNodeTypeRef("Node", "n"), ArraySeq(prop(varFor("n"), "name"))))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("graph type element referenced by '(:`Node` =>)' not found"),
      any()
    )
  }

  test("Rewrite invalid node reference in constraint should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(),
      Seq(keyConstraint(nodeTypeRefByVar("n"), ArraySeq(prop(varFor("n"), "name"))))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("graph type element referenced by 'n' not found"),
      any()
    )
  }

  test("Rewrite invalid edge reference in constraint should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(),
      Seq(keyConstraint(edgeTypeRefByVar("r"), ArraySeq(prop(varFor("r"), "name"))))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("graph type element referenced by 'r' not found"),
      any()
    )
  }

  test("Rewrite constraint into duplicate constraint should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(nodeTypeWithConstraints("Node", "n", Set(KeyConstraint(ArraySeq(prop(varFor("n"), "name")))(defaultPos)))),
      Seq(keyConstraint(nodeTypeRefByVar("n"), ArraySeq(prop(varFor("n"), "name"))))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("An equivalent constraint already exists"),
      any()
    )
  }

  test("Rewrite constraint into clashing constraint should throw exception") {
    // Given
    when(mockExceptionFactory.syntaxException(
      any[ErrorGqlStatusObject](),
      anyString(),
      any[InputPosition]()
    )).thenReturn(new RuntimeException())
    val gt = alterCurrentGraphTypeSet(graphType(
      Seq(nodeTypeWithConstraints(
        "Node",
        "n",
        Set(UniquenessConstraint(ArraySeq(prop(varFor("n"), "name")))(defaultPos))
      )),
      Seq(keyConstraint(nodeTypeRefByVar("n"), ArraySeq(prop(varFor("n"), "name"))))
    ))

    // When
    intercept[RuntimeException](rewriter.apply(gt))

    // Then
    verify(mockExceptionFactory).syntaxException(
      any(),
      ArgumentMatchers.startsWith("Conflicting constraint already exists"),
      any()
    )
  }

  // Run through all the standard examples, and make sure they are rewritten without any errors
  GraphTypeTestCase.testcases.foreach { tc =>
    test(s"${tc.name} should rewrite with no errors") {
      val rewritten = rewriter(alterCurrentGraphTypeSet(tc.ast))
      NoInlineConstraints.apply(rewritten)(NeverCancelled) shouldBe empty
      NoReferenceEqualityAmongVariables.apply(rewritten)(NeverCancelled) shouldBe empty
    }
  }
}
