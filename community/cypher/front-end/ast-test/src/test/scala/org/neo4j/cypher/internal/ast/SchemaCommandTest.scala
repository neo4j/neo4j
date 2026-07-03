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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.CypherVersionHelpers.versionedSemanticContext
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.collection.immutable.ArraySeq

class SchemaCommandTest extends CypherFunSuite with AstConstructionTestSupport with CypherVersionTestSupport {

  private val initialState = SemanticState.clean.withFeature(SemanticFeature.GraphTypes)

  private val p = InputPosition.withLength(13, 12, 11, 10)
  private val p2 = InputPosition.withLength(42, 7, 8, 9)
  private val p3 = InputPosition.withLength(11, 6, 9, 12)
  private val p4 = InputPosition.withLength(7, 13, 17, 23)
  private val pos1 = InputPosition(2, 1, 3).withInputLength(2)
  private val pos2 = InputPosition(16, 5, 4).withInputLength(4)
  private val pos3 = InputPosition(23, 7, 6).withInputLength(2)
  private val pos4 = InputPosition(37, 8, 9).withInputLength(2)
  private val pos5 = InputPosition(50, 6, 11).withInputLength(2)

  // INTEGER | FLOAT NOT NULL: mix of nullable and not nullable inner types of union
  private val invalidUnionType =
    ClosedDynamicUnionType(Set(IntegerType(isNullable = true)(pos2), FloatType(isNullable = false)(pos3)))(pos1)

  // LIST<VECTOR<INTEGER NOT NULL> | BOOLEAN NOT NULL> NOT NULL: mix of nullable and not nullable inner types of union
  private val invalidListType = ListType(
    ClosedDynamicUnionType(Set(
      VectorType(Some(IntegerType(isNullable = false)(pos3)), None, isNullable = true)(pos2),
      BooleanType(isNullable = false)(pos4)
    ))(pos5),
    isNullable = false
  )(pos1)

  private def getSemanticErrorMixOfNullability(
    constraintDescr: String,
    typeDescr: String,
    haveDifferentNullabilityCause: Boolean = false,
    positionFullType: InputPosition = pos1,
    positionUnionType: InputPosition = pos1
  ) = {
    val unsupportedInConstraintError = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
      .withParam(GqlParams.StringParam.valueType, typeDescr)

    val cause = if (haveDifferentNullabilityCause)
      unsupportedInConstraintError
        .atPosition(positionUnionType.offset, positionUnionType.line, positionUnionType.column)
        .withCause(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N63)
            .atPosition(positionUnionType.offset, positionUnionType.line, positionUnionType.column)
            .build()
        )
    else unsupportedInConstraintError

    SemanticError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
        .withParam(GqlParams.StringParam.constrDescrOrName, s"$constraintDescr constraint")
        .withCause(cause.build())
        .build(),
      s"Failed to create $constraintDescr constraint: Invalid property type `$typeDescr`.",
      positionFullType
    )
  }

  private def getSemanticErrorErrorInList(
    constraintDescr: String,
    typeDescr: String,
    gqlInvalidListReason: String,
    oldMessageInvalidListReason: String,
    position: InputPosition = pos1
  ) = SemanticError(
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
      .withParam(GqlParams.StringParam.constrDescrOrName, s"$constraintDescr constraint")
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
          .withParam(GqlParams.StringParam.valueType, typeDescr)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
              .withParam(GqlParams.StringParam.typeDescription, gqlInvalidListReason)
              .build()
          ).build()
      ).build(),
    s"Failed to create $constraintDescr constraint: Invalid property type `$typeDescr`. $oldMessageInvalidListReason",
    position
  )

  // Tests all Cypher versions

  testVersions("Create node property type constraint with invalid union type") { version =>
    val ast = CreateConstraint.createNodePropertyTypeConstraint(
      varFor("n"),
      labelName("L"),
      prop("n", "p", p2),
      invalidUnionType,
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe Seq(
      getSemanticErrorMixOfNullability("node property type", "INTEGER | FLOAT NOT NULL")
    )
  }

  testVersions("Create relationship property type constraint with invalid list type") { version =>
    val ast = CreateConstraint.createRelationshipPropertyTypeConstraint(
      varFor("r"),
      relTypeName("T"),
      prop("r", "p", p2),
      invalidListType,
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    val expectedErrors = Seq(
      // Mix of nullable and not nullable types in union
      getSemanticErrorMixOfNullability(
        "relationship property type",
        "LIST<BOOLEAN NOT NULL | VECTOR<INTEGER NOT NULL>> NOT NULL"
      ),
      // Cannot have union types in lists for property type constraints
      getSemanticErrorErrorInList(
        "relationship property type",
        "LIST<BOOLEAN | VECTOR<INTEGER NOT NULL>> NOT NULL",
        "a union of types",
        "Lists cannot have a union of types as an inner type."
      )
    )

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe expectedErrors
  }

  // Tests skipping Cypher 5

  testVersionsExcept5("Create node property type constraint in graph type with invalid list type") { version =>
    val ast = AlterCurrentGraphType(
      GraphType(
        Set.empty,
        Set(
          GraphTypeConstraintDefinition(
            None,
            NodeTypeReferenceByLabel(labelName("L"), Some(varFor("n")))(p),
            GraphTypeConstraint.PropertyTypeConstraint(ArraySeq(prop("n", "p", p2)), invalidListType)(p3),
            NoOptions
          )(p4)
        )
      )(p),
      AlterCurrentGraphType.Set
    )(p)

    val expectedErrors = Seq(
      // Mix of nullable and not nullable types in union
      getSemanticErrorMixOfNullability("graph type", "LIST<BOOLEAN NOT NULL | VECTOR<INTEGER NOT NULL>> NOT NULL"),
      // Cannot have union types in lists for property type constraints
      getSemanticErrorErrorInList(
        "graph type",
        "LIST<BOOLEAN | VECTOR<INTEGER NOT NULL>> NOT NULL",
        "a union of types",
        "Lists cannot have a union of types as an inner type."
      )
    )

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe expectedErrors
  }

  testVersionsExcept5("Create relationship property type constraint in graph type with invalid union type") { version =>
    val ast = AlterCurrentGraphType(
      GraphType(
        Set.empty,
        Set(
          GraphTypeConstraintDefinition(
            None,
            EdgeTypeReferenceByLabel(relTypeName("T"), Some(varFor("r")))(p),
            GraphTypeConstraint.PropertyTypeConstraint(ArraySeq(prop("r", "p", p2)), invalidUnionType)(p3),
            NoOptions
          )(p4)
        )
      )(p),
      AlterCurrentGraphType.Add
    )(p)

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe Seq(
      getSemanticErrorMixOfNullability("graph type", "INTEGER | FLOAT NOT NULL")
    )
  }

  testVersionsExcept5("Create property type in node element type with invalid union type") { version =>
    val ast = AlterCurrentGraphType(
      GraphType(
        Set(
          NodeType(
            None,
            labelName("L"),
            Set.empty,
            Set(PropertyType(propName("p", p3), invalidUnionType, None)(p4)),
            Set.empty
          )(p2)
        ),
        Set.empty
      )(p),
      AlterCurrentGraphType.Alter
    )(p)

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe Seq(
      getSemanticErrorMixOfNullability(
        "graph type",
        "INTEGER | FLOAT NOT NULL",
        haveDifferentNullabilityCause = true
      )
    )
  }

  testVersionsExcept5("Create property type in relationship element type with invalid list type") { version =>
    val ast = AlterCurrentGraphType(
      GraphType(
        Set(
          EdgeType(
            EmptyNodeTypeReference()(p),
            None,
            relTypeName("T"),
            Set(PropertyType(propName("p", p3), invalidListType, None)(p4)),
            EmptyNodeTypeReference()(p),
            Set.empty
          )(p2)
        ),
        Set.empty
      )(p),
      AlterCurrentGraphType.Set
    )(p)

    val expectedErrors = Seq(
      // Mix of nullable and not nullable types in union
      getSemanticErrorMixOfNullability(
        "graph type",
        "LIST<BOOLEAN NOT NULL | VECTOR<INTEGER NOT NULL>> NOT NULL",
        haveDifferentNullabilityCause = true,
        positionUnionType = pos5
      ),
      // Cannot have union types in lists for property type constraints
      getSemanticErrorErrorInList(
        "graph type",
        "LIST<BOOLEAN | VECTOR<INTEGER NOT NULL>> NOT NULL",
        "a union of types",
        "Lists cannot have a union of types as an inner type."
      )
    )

    ast.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe expectedErrors
  }

  // LOOKUP index function checks

  testVersions("Create node lookup index with a shadowed labels() function should fail semantic checking") { version =>
    val ast = CreateIndex.createLookupIndex(
      varFor("n"),
      isNodeIndex = true,
      function("labels", varFor("n")).copy(isShadowed = true)(p),
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    val result = ast.semanticCheck.run(initialState, versionedSemanticContext(version))
    result.errors.size shouldBe 1
    result.errors.head.msg shouldBe
      "Failed to create node lookup index: Function 'labels' is not allowed, valid function is 'labels'."
  }

  testVersions(
    "Create relationship lookup index with a shadowed type() function should fail semantic checking"
  ) { version =>
    val ast = CreateIndex.createLookupIndex(
      varFor("r"),
      isNodeIndex = false,
      function("type", varFor("r")).copy(isShadowed = true)(p),
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    val result = ast.semanticCheck.run(initialState, versionedSemanticContext(version))
    result.errors.size shouldBe 1
    result.errors.head.msg shouldBe
      "Failed to create relationship lookup index: Function 'type' is not allowed, valid function is 'type'."
  }

  testVersions(
    "Create node lookup index with a namespaced labels() function should fail semantic checking with the full name"
  ) { version =>
    val ast = CreateIndex.createLookupIndex(
      varFor("n"),
      isNodeIndex = true,
      function(Seq("foo"), "labels", varFor("n")),
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    val result = ast.semanticCheck.run(initialState, versionedSemanticContext(version))
    result.errors.size shouldBe 1
    result.errors.head.msg shouldBe
      "Failed to create node lookup index: Function 'foo.labels' is not allowed, valid function is 'labels'."
  }

  testVersions(
    "Create relationship lookup index with a namespaced type() function should fail semantic checking with the full name"
  ) { version =>
    val ast = CreateIndex.createLookupIndex(
      varFor("r"),
      isNodeIndex = false,
      function(Seq("foo"), "type", varFor("r")),
      None,
      IfExistsThrowError,
      NoOptions
    )(p)

    val result = ast.semanticCheck.run(initialState, versionedSemanticContext(version))
    result.errors.size shouldBe 1
    result.errors.head.msg shouldBe
      "Failed to create relationship lookup index: Function 'foo.type' is not allowed, valid function is 'type'."
  }
}
