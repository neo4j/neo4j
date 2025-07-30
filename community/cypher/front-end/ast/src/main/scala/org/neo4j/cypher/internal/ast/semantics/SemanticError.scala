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

import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

sealed trait SemanticErrorDef {
  def msg: String
  def position: InputPosition
  def withMsg(message: String): SemanticErrorDef
}

final case class SemanticError(gqlStatusObject: ErrorGqlStatusObject, msg: String, position: InputPosition)
    extends SemanticErrorDef {
  def this(msg: String, position: InputPosition) = this(null, msg, position)
  override def withMsg(message: String): SemanticError = copy(msg = message)
}

object SemanticError {

  def apply(msg: String, position: InputPosition): SemanticError = new SemanticError(null, msg, position)

  def unapply(errorDef: SemanticErrorDef): Option[(String, InputPosition)] = Some((errorDef.msg, errorDef.position))

  def invalidOption(schemaString: String, invalidOptions: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      invalidOptions,
      GqlParams.StringParam.input.process("OPTIONS"),
      java.util.List.of("indexProvider", "indexConfig"),
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      s"Failed to create $schemaString: Invalid option provided, valid options are `indexProvider` and `indexConfig`.",
      position
    )
  }

  def authForbidsClauseError(
    provider: String,
    unsupportedClause: String,
    expected: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      unsupportedClause,
      "auth provider " + GqlParams.StringParam.input.process(provider) + " attribute",
      expected,
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      s"Auth provider `$provider` does not allow `$unsupportedClause` clause.",
      position
    )
  }

  def unsupportedActionAccess(
    actionName: String,
    expectedActions: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      actionName,
      "property value access rules",
      expectedActions,
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, s"$actionName is not supported for property value access rules.", position)
  }

  def yieldMissingColumn(
    originalName: String,
    expectedColumns: java.util.List[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      originalName,
      "column name",
      expectedColumns,
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, s"Trying to YIELD non-existing column: `$originalName`", position)
  }

  def invalidFunctionForIndex(
    entityIndexDescription: String,
    name: String,
    validFunction: String,
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_22N04(
      name,
      "function name",
      java.util.List.of(validFunction),
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      s"Failed to create $entityIndexDescription: Function '$name' is not allowed, valid function is '$validFunction'.",
      position
    )
  }

  def invalidUseOfGraphFunction(graphFunction: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N75)
        .atPosition(pos.line, pos.column, pos.offset)
        .withParam(GqlParams.StringParam.fun, graphFunction)
        .build())
      .build()
    SemanticError(
      gql,
      s"`$graphFunction` is only allowed at the first position of a USE clause.",
      pos
    )
  }
  val existsErrorMessage = "The EXISTS expression is not valid in driver settings."
  val countErrorMessage = "The COUNT expression is not valid in driver settings."
  val collectErrorMessage = "The COLLECT expression is not valid in driver settings."
  val genericErrorMessage = "This expression is not valid in driver settings."

  def existsInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("EXISTS"),
      "driver settings",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, existsErrorMessage, position)
  }

  def countInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("COUNT"),
      "driver settings",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, countErrorMessage, position)
  }

  def collectInDriverSettings(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("COLLECT"),
      "driver settings",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, collectErrorMessage, position)
  }

  def genericDriverSettingsFail(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22N81(
      GqlParams.StringParam.cmd.process("EXISTS"),
      "driver settings",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, genericErrorMessage, position)
  }

  def cannotUseJoinHint(hint: UsingJoinHint, prettifiedHint: String): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
      .withParam(GqlParams.ListParam.hintList, Seq(prettifiedHint).asJava)
      .atPosition(hint.position.line, hint.position.column, hint.position.offset)
      .build()
    SemanticError(gql, "Cannot use join hint for single node pattern.", hint.position)
  }

  def variableAlreadyDeclaredInOuterScope(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N07(name, position.line, position.column, position.offset)
    SemanticError(gql, s"Variable `$name` already declared in outer scope", position)
  }

  def variableShadowingOuterScope(name: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N07(name, position.line, position.column, position.offset)
    SemanticError(
      gql,
      s"The variable `$name` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      position
    )
  }

  def legacyDisjunction(
    sanitizedLabelExpression: String,
    containsIs: Boolean,
    isNode: Boolean = false,
    position: InputPosition
  ): SemanticError = {
    val isOrColon = if (containsIs) "IS " else ":"
    val msg = if (isNode) {
      s"""Label expressions are not allowed to contain '|:'.
         |If you want to express a disjunction of labels, please use `$isOrColon$sanitizedLabelExpression` instead""".stripMargin
    } else {
      s"""The semantics of using colon in the separation of alternative relationship types in conjunction with
         |the use of variable binding, inlined property predicates, or variable length is no longer supported.
         |Please separate the relationships types using `$isOrColon$sanitizedLabelExpression` instead.""".stripMargin
    }
    val gql = GqlHelper.getGql42001_42I20(
      "|:",
      "|",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, msg, position)
  }

  def invalidDisjunction(isNode: Boolean, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I20(
      if (isNode) "|:" else ":",
      "|",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      if (isNode) "Label expressions are not allowed to contain '|:'."
      else "Relationship types in a relationship type expressions may not be combined using ':'",
      position
    )
  }

  def subPathAssignmentNotSupported(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N42(position.line, position.column, position.offset)
    SemanticError(gql, "Sub-path assignment is currently not supported.", position)
  }

  def unsupportedRequestOnSystemDatabase(
    invalidInput: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N17)
          .withParam(GqlParams.StringParam.input, invalidInput)
          .build()
      )
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidInput(
    wrongInput: String,
    forField: String,
    expectedInput: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
          .atPosition(pos.line, pos.column, pos.offset)
          .withParam(GqlParams.StringParam.input, wrongInput)
          .withParam(GqlParams.StringParam.context, forField)
          .withParam(GqlParams.ListParam.inputList, java.util.List.of(expectedInput))
          .build()
      )
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidEntityType(
    invalidInput: String,
    variable: String,
    expectedValueList: List[String],
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql22G03_22N27(
      invalidInput,
      variable,
      expectedValueList.asJava,
      pos.line,
      pos.column,
      pos.offset
    )

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def typeMismatch(
    expectedValueList: List[String],
    wrongType: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB1)
        .atPosition(pos.line, pos.column, pos.offset)
        .withParam(GqlParams.ListParam.valueTypeList, expectedValueList.asJava)
        .withParam(GqlParams.StringParam.input, wrongType)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidCoercion(
    cannotCoerceFrom: String,
    cannotCoerceTo: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N37)
        .atPosition(pos.line, pos.column, pos.offset)
        .withParam(GqlParams.StringParam.value, cannotCoerceFrom)
        .withParam(GqlParams.StringParam.valueType, cannotCoerceTo)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def specifiedNumberOutOfRange(
    component: String,
    valueType: String,
    lower: Number,
    upper: Number,
    inputValue: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N31)
        .atPosition(pos.line, pos.column, pos.offset)
        .withParam(GqlParams.StringParam.component, component)
        .withParam(GqlParams.StringParam.valueType, valueType)
        .withParam(GqlParams.NumberParam.lower, lower)
        .withParam(GqlParams.NumberParam.upper, upper)
        .withParam(GqlParams.StringParam.value, inputValue).build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def propertyTypeUnsupportedInConstraint(
    constraintTypeDescription: String,
    originalPropertyType: CypherType
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N11)
      .withParam(GqlParams.StringParam.constrDescrOrName, constraintTypeDescription + " constraint")
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N90)
        .withParam(GqlParams.StringParam.item, originalPropertyType.description)
        .build())
      .build()
    new SemanticError(
      gql,
      s"Failed to create ${constraintTypeDescription} constraint: " +
        s"Invalid property type `${originalPropertyType.description}`.",
      originalPropertyType.position
    )
  }

  def missingMandatoryAuthClause(
    clause: String,
    authProvider: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N97)
      .withParam(GqlParams.StringParam.clause, clause)
      .withParam(GqlParams.StringParam.auth, authProvider)
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def duplicateClause(clause: String, legacyMessage: String, pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N19)
      .withParam(GqlParams.StringParam.syntax, clause)
      .build()
    SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def missingHintPredicate(
    legacyMessage: String,
    hint: String,
    entity: String,
    variable: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
      .withParam(GqlParams.ListParam.hintList, Seq(hint).asJava)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N77)
        .withParam(GqlParams.StringParam.hint, hint)
        .withParam(GqlParams.StringParam.entityType, entity)
        .withParam(GqlParams.StringParam.variable, variable)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()
    SemanticError(gql, legacyMessage, position)
  }

  def functionRequiresWhereClause(func: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N70)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.fun, func)
        .build())
      .build()
    SemanticError(gql, s"$func(...) requires a WHERE predicate", position)
  }

  def aExpressionCannotContainUpdates(expr: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N57(expr, position.line, position.column, position.offset)
    SemanticError(gql, s"A $expr Expression cannot contain any updates", position)
  }

  def anExpressionCannotContainUpdates(expr: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N57(expr, position.line, position.column, position.offset)
    SemanticError(gql, s"An $expr Expression cannot contain any updates", position)
  }

  def singleReturnColumnRequired(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N22(position.line, position.column, position.offset)
    SemanticError(gql, "A Collect Expression must end with a single return column.", position)
  }

  def emptyListRangeOperator(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N20)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()
    SemanticError(gql, "The start or end (or both) is required for a collection slice", position)
  }

  def unboundVariablesInPatternExpression(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N29)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.variable, name)
        .build())
      .build()
    SemanticError(
      gql,
      s"PatternExpressions are not allowed to introduce new variables: '$name'.",
      position
    )
  }

  def incompatibleReturnColumns(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N39(position.line, position.column, position.offset)
    SemanticError(gql, "All sub queries in an UNION must have the same return column names", position)
  }

  def invalidUseOfUnion(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I40)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()

    SemanticError(gql, "Invalid combination of UNION and UNION ALL", position)
  }

  def invalidUseOfCIT(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I25(position.line, position.column, position.offset)
    SemanticError(
      gql,
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      position
    )
  }

  def invalidUseOfReturn(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I38)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()

    SemanticError(gql, s"$name can only be used at the end of the query.", position)
  }

  def invalidUseOfReturnStar(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I37)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()
    SemanticError(gql, "RETURN * is not allowed when there are no variables in scope", position)
  }

  def invalidUseOfMatch(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I31)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()

    SemanticError(
      gql,
      "MATCH cannot follow OPTIONAL MATCH (perhaps use a WITH clause between them)",
      position
    )
  }

  def invalidQuantifier(variables: Seq[String], position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I18)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.ListParam.variableList, variables.asJava)
        .build())
      .build()

    val errorMsg = implicitGroupingExpressionInAggregationColumnErrorMessage(variables)
    SemanticError(gql, errorMsg, position)
  }

  def invalidForeach(clause: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I01)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.clause, clause)
        .build())
      .build()

    SemanticError(gql, s"Invalid use of $clause inside FOREACH", position)
  }

  def unaliasedReturnItem(clause: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N21)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.clause, clause)
        .build())
      .build()

    SemanticError(gql, s"Expression in $clause must be aliased (use AS)", position)
  }

  def implicitGroupingExpressionInAggregationColumnErrorMessage(variables: Seq[String]): String =
    "Aggregation column contains implicit grouping expressions. " +
      "For example, in 'RETURN n.a, n.a + n.b + count(*)' the aggregation expression 'n.a + n.b + count(*)' includes the implicit grouping key 'n.b'. " +
      "It may be possible to rewrite the query by extracting these grouping/aggregation expressions into a preceding WITH clause. " +
      s"Illegal expression(s): ${variables.mkString(", ")}"

  def accessingMultipleGraphsError(legacyMessage: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA5)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build();
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def numberTooLarge(numberType: String, value: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql22003(value, position.line, position.column, position.offset)
    SemanticError(gql, s"$numberType is too large", position)
  }

  def notSupported(pos: InputPosition): SemanticError = {
    val msg = "Not supported."
    val gql = GqlHelper.get50N00(SemanticError.getClass.getSimpleName, msg, pos.line, pos.column, pos.offset)
    SemanticError(gql, msg, pos)
  }

  def bothOrReplaceAndIfNotExists(entity: String, userAsString: String, position: InputPosition) = {
    val gql = GqlHelper.getGql42001_42N14(
      "OR REPLACE",
      "IF NOT EXISTS",
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      s"Failed to create the specified $entity '$userAsString': cannot have both `OR REPLACE` and `IF NOT EXISTS`.",
      position
    )
  }

  def badCommandWithOrReplace(cmd: String, cypherCmd: String, position: InputPosition) = {
    val gql = GqlHelper.getGql42001_42N14("OR REPLACE", cypherCmd, position.line, position.column, position.offset)
    SemanticError(gql, s"Failed to $cmd: `OR REPLACE` cannot be used together with this command.", position)
  }

  def denyMergeUnsupported(position: InputPosition) = {
    val gql = GqlHelper.getGql42001_42N14("DENY", "MERGE", position.line, position.column, position.offset)
    SemanticError(gql, "`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
  }

  def grantDenyRevokeUnsupported(cmd: String, position: InputPosition) = {
    val gql = GqlHelper.getGql42001_42N14(
      "GRANT, DENY and REVOKE",
      cmd,
      position.line,
      position.column,
      position.offset
    )
    SemanticError(gql, s"`GRANT`, `DENY` and `REVOKE` are not supported for `$cmd`", position)
  }

  def unableToRouteUseClauseError(legacyMessage: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N04)
      .atPosition(position.line, position.column, position.offset)
      .withParam(GqlParams.StringParam.clause, "`USE` clause")
      .build()
    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def invalidNumberOfProcedureOrFunctionArguments(
    expectedNumberOfArgs: Int,
    obtainedNumberOfArgs: Int,
    procedureFunction: String,
    signature: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.NumberParam.count1, expectedNumberOfArgs)
        .withParam(GqlParams.NumberParam.count2, obtainedNumberOfArgs)
        .withParam(GqlParams.StringParam.procFun, procedureFunction)
        .withParam(GqlParams.StringParam.sig, signature)
        .build())
      .build()

    SemanticError(
      gql,
      legacyMessage,
      position
    )
  }

  def invalidYieldStar(
    commandName: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N94)
      .build()
    SemanticError(
      gql,
      s"When combining `${commandName}` with other show and/or terminate commands, `YIELD *` isn't permitted.",
      position
    )
  }

  def missingYield(
    commandName: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N93)
      .build()
    SemanticError(
      gql,
      s"When combining `${commandName}` with other show and/or terminate commands, `YIELD` is mandatory.",
      position
    )
  }

  def missingReturn(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N92)
      .build()
    SemanticError(
      gql,
      "When combining show and/or terminate commands, `RETURN` isn't optional.",
      position
    )
  }

  def queryMustConcludeWithClause(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.line, position.column, position.offset)
    SemanticError(gql, s"Query must conclude with $validLastClauses.", position)
  }

  def queryCannotConcludeWithCall(callName: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.line, position.column, position.offset)
    SemanticError(gql, s"Query cannot conclude with $callName together with YIELD", position)
  }

  def queryCannotConcludeWithClause(clause: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N71(position.line, position.column, position.offset)
    SemanticError(
      gql,
      s"Query cannot conclude with $clause (must be $validLastClauses).",
      position
    )
  }

  private val validLastClauses =
    "a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD"

  def withIsRequiredBetween(clause1: String, clause2: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42N24(clause1, clause2, position.line, position.column, position.offset)
    SemanticError(gql, s"WITH is required between $clause1 and $clause2", position)
  }

  def invalidType(
    value: String,
    correctTypes: List[String],
    actualType: String,
    legacyMessage: String,
    pos: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
        .atPosition(pos.line, pos.column, pos.offset)
        .withParam(GqlParams.StringParam.value, value)
        .withParam(GqlParams.ListParam.valueTypeList, correctTypes.asJava)
        .withParam(GqlParams.StringParam.valueType, actualType)
        .build())
      .build()

    new SemanticError(
      gql,
      legacyMessage,
      pos
    )
  }

  def invalidPlacementOfUseClause(pos: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(pos.line, pos.column, pos.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N73)
        .atPosition(pos.line, pos.column, pos.offset)
        .build())
      .build()

    new SemanticError(
      gql,
      "USE clause must be the first clause in a (sub-)query.",
      pos
    )
  }

  def invalidSubqueryInMerge(position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I48(position.line, position.column, position.offset)
    SemanticError(gql, "Subquery expressions are not allowed in a MERGE clause.", position)
  }

  def invalidUseOfMultiplePathPatterns(matchModeAvailable: Boolean, position: InputPosition): SemanticError = {
    val baseMessage =
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector."

    // Let's only mention match modes when that is an available feature
    val action = if (matchModeAvailable) {
      " You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode."
    } else {
      ""
    }

    val gql = GqlHelper.getGql42001_42I45(action, position.line, position.column, position.offset)
    SemanticError(gql, baseMessage + action, position)
  }

  def invalidFieldTerminator(position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I05)
        .atPosition(position.line, position.column, position.offset)
        .build())
      .build()
    SemanticError(gql, "CSV field terminator can only be one character wide", position)
  }

  def singleRelationshipPatternRequired(name: String, position: InputPosition): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(position.line, position.column, position.offset)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N40)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.fun, name)
        .build())
      .build()

    SemanticError(gql, s"$name(...) requires a pattern containing a single relationship", position)
  }

  def inputContainsInvalidCharacters(
    invalidInput: String,
    context: String,
    legacyMessage: String,
    position: InputPosition
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
      .atPosition(position.line, position.column, position.offset)
      .withParam(GqlParams.StringParam.input, invalidInput)
      .withParam(GqlParams.StringParam.context, context)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N82)
        .atPosition(position.line, position.column, position.offset)
        .withParam(GqlParams.StringParam.input, invalidInput)
        .withParam(GqlParams.StringParam.context, context)
        .build())
      .build()
    SemanticError(gql, legacyMessage, position)
  }

  def mixingColonAndIs(
    labelExpressions: Set[String],
    replacements: Set[String],
    position: InputPosition
  ): SemanticError = {
    val gql = GqlHelper.getGql42001_42I29(
      labelExpressions.mkString(", "),
      replacements.mkString(", "),
      position.line,
      position.column,
      position.offset
    )
    val exprText = if (replacements.size > 1) "These expressions" else "This expression"
    SemanticError(
      gql,
      s"Mixing the IS keyword with colon (':') between labels is not allowed. $exprText could be expressed as ${replacements.mkString(", ")}.",
      position
    )
  }

  def mixingIsWithMultipleLabels(statement: String, replacement: String, position: InputPosition): SemanticError = {
    val gql = GqlHelper.getGql42001_42I29(
      statement,
      replacement,
      position.line,
      position.column,
      position.offset
    )
    SemanticError(
      gql,
      s"It is not supported to use the `IS` keyword together with multiple labels in `$statement`. Rewrite the expression as `$replacement`.",
      position
    )
  }
}

sealed trait UnsupportedOpenCypher extends SemanticErrorDef

final case class FeatureError(
  gqlStatusObject: ErrorGqlStatusObject,
  msg: String,
  feature: SemanticFeature,
  position: InputPosition
) extends UnsupportedOpenCypher {

  def this(msg: String, featureError: SemanticFeature, position: InputPosition) =
    this(null, msg, featureError, position)
  override def withMsg(message: String): FeatureError = copy(msg = message)
}

object FeatureError {

  def apply(msg: String, featureError: SemanticFeature, position: InputPosition): FeatureError =
    new FeatureError(null, msg, featureError, position)

  def unapply(errorDef: FeatureError): Option[(String, SemanticFeature, InputPosition)] =
    Some((errorDef.msg, errorDef.feature, errorDef.position))
}
