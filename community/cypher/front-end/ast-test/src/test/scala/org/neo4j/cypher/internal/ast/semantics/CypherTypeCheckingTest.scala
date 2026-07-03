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

import org.neo4j.cypher.internal.CypherVersionHelpers.arbitrarySemanticContext
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.alterCurrentGraphTypeSet
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.edgeType
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.edgeTypeRefByLabel
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.graphType
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.nodeType
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.nodeTypeRefByLabel
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.propertyTypeConstraint
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase.propertyTypeWithPos
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.PropertyValueCypher5Type
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.collection.immutable.ArraySeq

class CypherTypeCheckingTest extends CypherFunSuite with AstConstructionTestSupport {
  private val pos1 = InputPosition(2, 1, 3)
  private val pos2 = InputPosition(16, 5, 4)
  private val pos3 = InputPosition(23, 7, 6)
  private val pos4 = InputPosition(37, 8, 9)
  private val pos5 = InputPosition(42, 11, 14)

  private val initialState = SemanticState.clean.withFeature(SemanticFeature.GraphTypes)

  // Dependent constraints allow non-nullable types (including non-nullable lists).
  // Otherwise, the rules are the same for all sort of property type constraints.

  test("non-nullable property type should not be valid for legacy property type constraint") {
    val constraintCommand = nodePropertyTypeConstraint(Legacy, IntegerType(isNullable = false)(pos1), pos2)
    assertPropertyTypeConstraintError(constraintCommand, Legacy, "node", "INTEGER NOT NULL", pos1)
  }

  test("non-nullable property type should be not be valid for independent property type constraint") {
    val constraintCommand = nodePropertyTypeConstraint(Independent, IntegerType(isNullable = false)(pos1), pos2)
    assertPropertyTypeConstraintError(constraintCommand, Independent, "node", "INTEGER NOT NULL", pos1)
  }

  test("non-nullable property type should be valid for dependent property type constraint") {
    val constraintCommand = nodePropertyTypeConstraint(Dependent, IntegerType(isNullable = false)(pos1), pos2)
    constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
  }

  test("non-nullable list of property type should not be valid for legacy property type constraint") {
    val constraintCommand =
      relPropertyTypeConstraint(
        Legacy,
        ListType(StringType(isNullable = false)(pos1), isNullable = false)(pos2),
        pos3
      )

    assertPropertyTypeConstraintError(
      constraintCommand,
      Legacy,
      "relationship",
      "LIST<STRING NOT NULL> NOT NULL",
      pos2
    )
  }

  test("non-nullable list of property type should not be valid for independent property type constraint") {
    val constraintCommand =
      relPropertyTypeConstraint(
        Independent,
        ListType(StringType(isNullable = false)(pos1), isNullable = false)(pos2),
        pos3
      )

    assertPropertyTypeConstraintError(
      constraintCommand,
      Independent,
      "relationship",
      "LIST<STRING NOT NULL> NOT NULL",
      pos2
    )
  }

  test("non-nullable list of property type should be valid for dependent property type constraint") {
    val constraintCommand =
      relPropertyTypeConstraint(
        Dependent,
        ListType(StringType(isNullable = false)(pos1), isNullable = false)(pos2),
        pos3
      )

    constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
  }

  Seq(Legacy, Dependent, Independent).foreach {
    constraintType =>
      test(s"nullable property type should be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(constraintType, IntegerType(isNullable = true)(pos1), pos2)
        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"non-property type should not be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(constraintType, NodeType(isNullable = true)(pos1), pos2)
        assertPropertyTypeConstraintError(constraintCommand, constraintType, "node", "NODE", pos1)
      }

      // Vector types

      test(s"vector should be valid for node property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          VectorType(Some(IntegerType(isNullable = false)(pos1)), Some(512), isNullable = true)(pos2),
          pos3
        )
        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"vector should be valid for relationship property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          VectorType(Some(Float32Type(isNullable = false)(pos1)), Some(12), isNullable = true)(pos2),
          pos3
        )
        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"vector with omitted dimension should not be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          VectorType(Some(Integer16Type(isNullable = false)(pos1)), None, isNullable = true)(pos2),
          pos3
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "VECTOR<INTEGER16 NOT NULL>",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBA).build(),
          "Property type constraints for vectors need to define both coordinate type and dimension.",
          pos2
        )
      }

      test(s"vector with omitted coordinate type should not be valid for property type constraint - $constraintType") {
        val constraintCommand =
          relPropertyTypeConstraint(constraintType, VectorType(None, Some(42), isNullable = true)(pos1), pos2)

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "relationship",
          "VECTOR(42)",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBA).build(),
          "Property type constraints for vectors need to define both coordinate type and dimension.",
          pos1
        )
      }

      test(
        s"vector with omitted dimension and coordinate type should not be valid for property type constraint - $constraintType"
      ) {
        val constraintCommand =
          nodePropertyTypeConstraint(constraintType, VectorType(None, None, isNullable = true)(pos1), pos2)

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "VECTOR",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBA).build(),
          "Property type constraints for vectors need to define both coordinate type and dimension.",
          pos1
        )
      }

      test(s"vector with negative dimension should not be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          VectorType(Some(FloatType(isNullable = false)(pos1)), Some(-1), isNullable = true)(pos2),
          pos3
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "relationship",
          "VECTOR<FLOAT NOT NULL>(-1)",
          dimensionErrorCause(-1),
          "The dimension of property type constraints for vectors needs to be between 1 and 4096.",
          pos2
        )
      }

      test(s"vector with zero dimension should not be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          VectorType(Some(IntegerType(isNullable = false)(pos1)), Some(0), isNullable = true)(pos2),
          pos3
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "VECTOR<INTEGER NOT NULL>(0)",
          dimensionErrorCause(0),
          "The dimension of property type constraints for vectors needs to be between 1 and 4096.",
          pos2
        )
      }

      test(s"vector with one dimension should be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          VectorType(Some(Integer32Type(isNullable = false)(pos1)), Some(1), isNullable = true)(pos2),
          pos3
        )
        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"vector with dimension 4096 should be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          VectorType(Some(Integer8Type(isNullable = false)(pos1)), Some(4096), isNullable = true)(pos2),
          pos3
        )
        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"vector with too large dimension should not be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          VectorType(Some(Float32Type(isNullable = false)(pos1)), Some(4097), isNullable = true)(pos2),
          pos3
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "relationship",
          "VECTOR<FLOAT32 NOT NULL>(4097)",
          dimensionErrorCause(4097),
          "The dimension of property type constraints for vectors needs to be between 1 and 4096.",
          pos2
        )
      }

      // List types

      test(
        s"nullable list of non-nullable, non-vector property type should be valid for property type constraint - $constraintType"
      ) {
        val constraintCommand =
          relPropertyTypeConstraint(
            constraintType,
            ListType(StringType(isNullable = false)(pos1), isNullable = true)(pos2),
            pos3
          )

        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(
        s"nullable list of nullable property type should not be valid for property type constraint - $constraintType"
      ) {
        val constraintCommand =
          nodePropertyTypeConstraint(
            constraintType,
            ListType(FloatType(isNullable = true)(pos1), isNullable = true)(pos2),
            pos3
          )

        val expectedCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
          .withParam(GqlParams.StringParam.typeDescription, "a nullable type")
          .build()

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "LIST<FLOAT>",
          expectedCause,
          "Lists cannot have nullable inner types.",
          pos2
        )
      }

      test(s"list of vector should not be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          ListType(
            VectorType(Some(Float32Type(isNullable = false)(pos1)), Some(12), isNullable = false)(pos2),
            isNullable = true
          )(pos3),
          pos4
        )

        val expectedCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
          .withParam(GqlParams.StringParam.typeDescription, "a vector")
          .build()

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "relationship",
          "LIST<VECTOR<FLOAT32 NOT NULL>(12) NOT NULL>",
          expectedCause,
          "Lists cannot have a vector as an inner type.",
          pos3
        )
      }

      test(s"list of list should not be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ListType(ListType(FloatType(isNullable = false)(pos1), isNullable = true)(pos2), isNullable = true)(pos3),
          pos4
        )

        val expectedCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
          .withParam(GqlParams.StringParam.typeDescription, "a list")
          .build()

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "LIST<LIST<FLOAT NOT NULL>>",
          expectedCause,
          "Lists cannot have lists as an inner type.",
          pos3
        )
      }

      test(s"list of union should not be valid for property type constraint - $constraintType") {
        val constraintCommand = relPropertyTypeConstraint(
          constraintType,
          ListType(
            ClosedDynamicUnionType(Set(
              IntegerType(isNullable = false)(pos1),
              FloatType(isNullable = false)(pos2)
            ))(pos3),
            isNullable = true
          )(pos4),
          pos5
        )

        val expectedCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB9)
          .withParam(GqlParams.StringParam.typeDescription, "a union of types")
          .build()

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "relationship",
          "LIST<INTEGER NOT NULL | FLOAT NOT NULL>",
          expectedCause,
          "Lists cannot have a union of types as an inner type.",
          pos4
        )
      }

      // Union of types

      test(s"union of valid types should be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ClosedDynamicUnionType(Set(
            ListType(IntegerType(isNullable = false)(pos1), isNullable = true)(pos2),
            FloatType(isNullable = true)(pos3)
          ))(pos4),
          pos5
        )

        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(s"union with vector should be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ClosedDynamicUnionType(Set(
            ListType(IntegerType(isNullable = false)(pos1), isNullable = true)(pos2),
            VectorType(Some(IntegerType(isNullable = false)(pos3)), Some(3), isNullable = true)(pos4)
          ))(pos5),
          pos1
        )

        constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe empty
      }

      test(
        s"union with vector with omitted dimension should not be valid for property type constraint - $constraintType"
      ) {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ClosedDynamicUnionType(Set(
            ListType(IntegerType(isNullable = false)(pos1), isNullable = true)(pos2),
            VectorType(Some(IntegerType(isNullable = false)(pos3)), None, isNullable = true)(pos4)
          ))(pos5),
          pos1
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "VECTOR<INTEGER NOT NULL> | LIST<INTEGER NOT NULL>",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NBA).build(),
          "Property type constraints for vectors need to define both coordinate type and dimension.",
          pos5
        )
      }

      test(
        s"union with vector of invalid dimension should not be valid for property type constraint - $constraintType"
      ) {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ClosedDynamicUnionType(Set(
            ListType(IntegerType(isNullable = false)(pos1), isNullable = true)(pos2),
            VectorType(Some(IntegerType(isNullable = false)(pos3)), Some(0), isNullable = true)(pos4)
          ))(pos5),
          pos1
        )

        assertPropertyTypeConstraintErrorWithCause(
          constraintCommand,
          constraintType,
          "node",
          "VECTOR<INTEGER NOT NULL>(0) | LIST<INTEGER NOT NULL>",
          dimensionErrorCause(0),
          "The dimension of property type constraints for vectors needs to be between 1 and 4096.",
          pos5
        )
      }

      test(s"union with none valid type should not be valid for property type constraint - $constraintType") {
        val constraintCommand = nodePropertyTypeConstraint(
          constraintType,
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = false)(pos1),
            NodeType(isNullable = false)(pos2)
          ))(pos3),
          pos4
        )

        assertPropertyTypeConstraintError(
          constraintCommand,
          constraintType,
          "node",
          "INTEGER NOT NULL | NODE NOT NULL",
          pos3
        )
      }

      test(s"any property value should not be valid for property type constraint - $constraintType") {
        val propertyValueTypes = Seq(
          (PropertyValueType(isNullable = true)(pos1), "PROPERTY VALUE"),
          (PropertyValueCypher5Type(isNullable = true)(pos1), "PROPERTY VALUE"),
          (
            ListType(PropertyValueType(isNullable = false)(pos3), isNullable = true)(pos1),
            "LIST<PROPERTY VALUE NOT NULL>"
          ),
          (
            ListType(PropertyValueCypher5Type(isNullable = false)(pos3), isNullable = true)(pos1),
            "LIST<PROPERTY VALUE NOT NULL>"
          ),
          (
            ClosedDynamicUnionType(Set(
              IntegerType(isNullable = true)(pos4),
              PropertyValueType(isNullable = true)(pos3)
            ))(pos1),
            "INTEGER | PROPERTY VALUE"
          ),
          (
            ClosedDynamicUnionType(Set(
              IntegerType(isNullable = true)(pos4),
              PropertyValueCypher5Type(isNullable = true)(pos3)
            ))(pos1),
            "INTEGER | PROPERTY VALUE"
          )
        )

        propertyValueTypes.foreach { case (propertyType, typeString) =>
          withClue(s"PropertyType = $propertyType \n") {
            val constraintCommand = relPropertyTypeConstraint(constraintType, propertyType, pos2)
            assertPropertyTypeConstraintError(constraintCommand, constraintType, "relationship", typeString, pos1)
          }

        }
      }
  }

  // Help methods
  sealed trait ConstraintType
  case object Legacy extends ConstraintType
  case object Dependent extends ConstraintType
  case object Independent extends ConstraintType

  private def nodePropertyTypeConstraint(
    constraintType: ConstraintType,
    valueType: CypherType,
    position: InputPosition
  ): SchemaCommand = {
    constraintType match {
      case Legacy =>
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("n"),
          labelName("Label"),
          prop("n", "prop"),
          valueType,
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(position)
      case Dependent =>
        extractSchemaCommand(
          alterCurrentGraphTypeSet(
            graphType(
              nodeType(
                "Product",
                propertyTypeWithPos(
                  "feature",
                  valueType,
                  position
                )
              )
            )
          )
        )
      case Independent =>
        extractSchemaCommand(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(),
              Seq(
                propertyTypeConstraint(
                  "independentConstraint",
                  nodeTypeRefByLabel("Label", "n"),
                  ArraySeq(prop("n", "score")),
                  valueType
                )
              )
            )
          )
        )
    }
  }

  private def relPropertyTypeConstraint(
    constraintType: ConstraintType,
    valueType: CypherType,
    position: InputPosition
  ): SchemaCommand = {
    constraintType match {
      case Legacy =>
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          valueType,
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(position)
      case Dependent =>
        extractSchemaCommand(
          alterCurrentGraphTypeSet(
            graphType(
              edgeType(
                nodeTypeRefByLabel("User"),
                "INTERACTS",
                EmptyNodeTypeReference()(defaultPos),
                propertyTypeWithPos(
                  "score",
                  valueType,
                  position
                )
              )
            )
          )
        )
      case Independent =>
        extractSchemaCommand(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(),
              Seq(
                propertyTypeConstraint(
                  "c",
                  edgeTypeRefByLabel("REL", "r"),
                  ArraySeq(prop("r", "feat")),
                  valueType
                )
              )
            )
          )
        )
    }
  }

  /*
   * The AstConstructionTestSupport has an implicit conversion from Statement to Statements
   * This method converts back to Statement and casts to SchemaCommand
   */
  private def extractSchemaCommand(graphType: Statements): SchemaCommand = {
    graphType.get(0).asInstanceOf[SchemaCommand]
  }

  private def assertPropertyTypeConstraintError(
    constraintCommand: SchemaCommand,
    constraintType: ConstraintType,
    elementType: String,
    cypherType: String,
    position: InputPosition
  ): Unit = {
    val constraintDescr =
      if (constraintType.equals(Legacy)) s"$elementType property type constraint" else "graph type constraint"
    constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
            .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescr)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
              .withParam(GqlParams.StringParam.valueType, cypherType)
              .build())
            .build(),
          s"Failed to create $constraintDescr: Invalid property type `$cypherType`.",
          position
        )
      ).errors
  }

  private def assertPropertyTypeConstraintErrorWithCause(
    constraintCommand: SchemaCommand,
    constraintType: ConstraintType,
    elementType: String,
    cypherType: String,
    cause: ErrorGqlStatusObject,
    additionalErrorInfo: String,
    position: InputPosition
  ): Unit = {
    val constraintDescr =
      if (constraintType.equals(Legacy)) s"$elementType property type constraint" else "graph type constraint"
    constraintCommand.semanticCheck.run(initialState, arbitrarySemanticContext()).errors shouldBe SemanticCheckResult
      .error(
        initialState,
        SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
            .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescr)
            .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
              .withParam(GqlParams.StringParam.valueType, cypherType)
              .withCause(cause)
              .build())
            .build(),
          s"Failed to create $constraintDescr: Invalid property type `$cypherType`. " +
            additionalErrorInfo,
          position
        )
      ).errors
  }

  private def dimensionErrorCause(dimension: Int): ErrorGqlStatusObject = {
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N31)
      .withParam(GqlParams.StringParam.component, "DIMENSION")
      .withParam(GqlParams.StringParam.valueType, "INTEGER NOT NULL")
      .withParam(GqlParams.NumberParam.lower, 1)
      .withParam(GqlParams.NumberParam.upper, 4096)
      .withParam(GqlParams.StringParam.value, dimension.toString)
      .build()
  }
}
