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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.ActionResourceBase
import org.neo4j.cypher.internal.ast.AddTags
import org.neo4j.cypher.internal.ast.AddedInRewriteShowCommands
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AlterAuthRule
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUsers
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.AuthRuleEnabled
import org.neo4j.cypher.internal.ast.AuthRuleSetClause
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateAuthRule
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateFulltextIndex
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateReplicaDatabase
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateSingleLabelPropertyIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateVectorIndex
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAndDbmsAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DropAuthRule
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.Element
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ExpandHintAll
import org.neo4j.cypher.internal.ast.ExpandHintInto
import org.neo4j.cypher.internal.ast.ExpandHintMode
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FunctionAllQualifier
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToAuthRules
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelResource
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.ParsedAsLimit
import org.neo4j.cypher.internal.ast.ParsedAsOrderBy
import org.neo4j.cypher.internal.ast.ParsedAsSkip
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureAllQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.PropertyResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.Relationship
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveAllTags
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.RemoveTags
import org.neo4j.cypher.internal.ast.RenameAuthRule
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnAddedInRewrite
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromAuthRules
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SecretAllQualifier
import org.neo4j.cypher.internal.ast.SecretQualifier
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItems
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.SettingAllQualifier
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabasesClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SingleNamedGraphScope
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.UserTagsAction
import org.neo4j.cypher.internal.ast.UsingExpandHint
import org.neo4j.cypher.internal.ast.UsingExpandStepHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathAll
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathInto
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.BASE_INDENT
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.NL
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.authRuleSetClausesToString
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.escapeDatabaseName
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.stringifyOptions
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.userTagsActionAsString
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.util.Stringifier.backtick
import org.neo4j.util.Stringifier.backtickEmpty

import scala.annotation.unused

//noinspection DuplicatedCode
case class Prettifier(
  expr: ExpressionStringifier,
  extension: Prettifier.ClausePrettifier = Prettifier.EmptyExtension,
  useInCommands: Boolean = true
) {

  private val base = IndentingQueryPrettifier()

  def asString(statement: Statement): String = statement match {
    case q: Query                 => base.query(q)
    case c: SchemaCommand         => asString(c)
    case c: AdministrationCommand => asString(c)
    case _                        => throw new IllegalStateException(s"Unknown statement: $statement")
  }

  def asString(localCallableDefinition: LocalCallableDefinition): String = base.asString(localCallableDefinition)

  def asString(unresolvedCall: UnresolvedCall): String = base.asString(unresolvedCall)

  def asString(search: Search): String = base.asString(search)

  def asString(groupBy: GroupBy): String = base.asString(groupBy)

  def asString(hint: Hint): String = base.asString(hint)

  def asString(lfs: LocalFieldSignature): String = base.asString(lfs)

  def backtick(s: String): String = expr.backtick(s)

  private def propertiesMapToString(
    name: String,
    properties: Option[Either[Map[String, Expression], Parameter]]
  ): String =
    properties match {
      case Some(Left(props)) =>
        if (props.nonEmpty) {
          s" $name ${props.map({ case (s, e) => s"${backtickEmpty(s)}: ${expr(e)}" }).mkString("{", ", ", "}")}"
        } else {
          s" $name {}"
        }
      case Some(Right(parameter)) => s" $name ${expr(parameter)}"
      case None                   => ""
    }

  def prettifySetItems(setItems: Seq[SetItem]): String = {
    val items = setItems.map {
      case SetPropertyItem(prop, exp) =>
        s"${expr(prop, shouldBacktickEmpty = true)} = ${expr(exp, shouldBacktickEmpty = true)}"
      case SetDynamicPropertyItem(prop, exp) =>
        s"${expr(prop, shouldBacktickEmpty = true)} = ${expr(exp, shouldBacktickEmpty = true)}"
      case SetPropertyItems(entity, items) =>
        items
          .map(i =>
            s"${expr(entity, shouldBacktickEmpty = true)}.${backtickEmpty(i._1.name)} = ${expr(i._2, shouldBacktickEmpty = true)}"
          )
          .mkString(", ")
      case SetLabelItem(variable, labels, dynamicLabels, false) => labelsString(variable, labels, dynamicLabels)
      case SetLabelItem(variable, labels, dynamicLabels, true)  => isLabelsString(variable, labels, dynamicLabels)
      case SetIncludingPropertiesFromMapItem(variable, exp, _) =>
        s"${expr(variable, shouldBacktickEmpty = true)} += ${expr(exp, shouldBacktickEmpty = true)}"
      case SetExactPropertiesFromMapItem(variable, exp, _) =>
        s"${expr(variable, shouldBacktickEmpty = true)} = ${expr(exp, shouldBacktickEmpty = true)}"
    }
    items.mkString(", ")
  }

  def prettifyRemoveItems(removeItems: Seq[RemoveItem]): String = {
    val items = removeItems.map {
      case RemovePropertyItem(prop) => s"${expr(prop, shouldBacktickEmpty = true)}"
      case RemoveDynamicPropertyItem(dynamicPropertyLookup) =>
        s"${expr(dynamicPropertyLookup, shouldBacktickEmpty = true)}"
      case RemoveLabelItem(variable, labels, dynamicLabels, false) => labelsString(variable, labels, dynamicLabels)
      case RemoveLabelItem(variable, labels, dynamicLabels, true)  => isLabelsString(variable, labels, dynamicLabels)
    }
    items.mkString(", ")
  }

  private def labelsString(
    variable: LogicalVariable,
    labels: Seq[LabelName],
    dynamicLabels: Seq[Expression]
  ): String = {
    expr(variable) + labelsOrderedSeq(labels, dynamicLabels).map(l => s":$l").mkString("")
  }

  private def isLabelsString(
    variable: LogicalVariable,
    labels: Seq[LabelName],
    dynamicLabels: Seq[Expression]
  ): String = {
    val labelsStrings: Seq[String] = labelsOrderedSeq(labels, dynamicLabels)
    expr(variable) + " IS " + labelsStrings.head + labelsStrings.tail.map(l => s":$l").mkString("")
  }

  private def labelsOrderedSeq(labels: Seq[LabelName], dynamicLabels: Seq[Expression]): Seq[String] = {
    (labels ++ dynamicLabels).map {
      case l: LabelName  => (s"${expr(l)}", l.position)
      case d: Expression => (s"$$(${expr(d)})", d.position)
      case _             => throw new IllegalStateException("Unreachable state.")
    }.sortBy(pos => (pos._2.line, pos._2.column)).map(_._1)
  }

  def asString(command: SchemaCommand): String = {
    def propertiesToString(properties: Seq[Property]): String =
      properties.map(propertyToString).mkString("(", ", ", ")")
    def propertyToString(property: Property): String =
      s"${expr(property.map)}.${expr(property.propertyKey)}"

    def getStartOfCommand(
      name: Option[Expression],
      ifExistsDo: IfExistsDo,
      schemaType: String
    ): String = {
      val nameString = name.map(n => s"${Prettifier.escapeName(n)} ").getOrElse("")
      ifExistsDo match {
        case IfExistsDoNothing     => s"CREATE $schemaType ${nameString}IF NOT EXISTS "
        case IfExistsInvalidSyntax => s"CREATE OR REPLACE $schemaType ${nameString}IF NOT EXISTS "
        case IfExistsReplace       => s"CREATE OR REPLACE $schemaType $nameString"
        case IfExistsThrowError    => s"CREATE $schemaType $nameString"
      }
    }

    val useString = asString(command.useGraph)
    val commandString = command match {

      case CreateSingleLabelPropertyIndex(
          Variable(variable),
          entityName,
          properties,
          name,
          indexType,
          ifExistsDo,
          options
        ) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val anyAll: Boolean => String = a => if (a) "all" else "any"
        val pattern = entityName match {
          case LabelName(label)     => s"(${backtickEmpty(variable)}:${backtickEmpty(label)})"
          case RelTypeName(relType) => s"()-[${backtickEmpty(variable)}:${backtickEmpty(relType)}]-()"
          case DynamicLabelExpression(expression, all) =>
            s"(${backtickEmpty(variable)}:${anyAll(all)}$$(${expr(expression)}))"
          case DynamicRelTypeExpression(expression, all) =>
            s"()-[${backtickEmpty(variable)}:${anyAll(all)}$$(${expr(expression)})]-()"
        }
        s"${startOfCommand}FOR $pattern ON ${propertiesToString(properties)}${stringifyOptions(options)(expr)}"

      case CreateLookupIndex(Variable(variable), isNodeIndex, function, name, indexType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val pattern = if (isNodeIndex) s"(${backtickEmpty(variable)})" else s"()-[${backtickEmpty(variable)}]-()"
        // can't use `expr(functions)` since that might add extra () we can't parse: labels((n))
        val functionString =
          function.name + "(" + function.args.map(e => backtickEmpty(e.asCanonicalStringVal)).mkString(", ") + ")"
        s"${startOfCommand}FOR $pattern ON EACH $functionString${stringifyOptions(options)(expr)}"

      case CreateFulltextIndex(Variable(variable), entityNames, properties, name, indexType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val pattern = entityNames match {
          case Left(labels) =>
            val labelPattern = labels.map(l => backtickEmpty(l.name)).mkString(":", "|", "")
            s"(${backtickEmpty(variable)}$labelPattern)"
          case Right(relTypes) =>
            val relTypePattern = relTypes.map(r => backtickEmpty(r.name)).mkString(":", "|", "")
            s"()-[${backtickEmpty(variable)}$relTypePattern]-()"
        }
        val propertiesString = properties.map(propertyToString).mkString("[", ", ", "]")
        s"${startOfCommand}FOR $pattern ON EACH $propertiesString${stringifyOptions(options)(expr)}"

      case CreateVectorIndex(
          Variable(variable),
          entityNames,
          properties,
          additionalProperties,
          name,
          indexType,
          ifExistsDo,
          options
        ) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val pattern = entityNames match {
          case Left(labels) =>
            val labelPattern = labels.map(l => backtickEmpty(l.name)).mkString(":", "|", "")
            s"(${backtickEmpty(variable)}$labelPattern)"
          case Right(relTypes) =>
            val relTypePattern = relTypes.map(r => backtickEmpty(r.name)).mkString(":", "|", "")
            s"()-[${backtickEmpty(variable)}$relTypePattern]-()"
        }
        val additionalPropertiesString =
          if (additionalProperties.nonEmpty) additionalProperties.map(propertyToString).mkString(" WITH [", ", ", "]")
          else ""
        s"${startOfCommand}FOR $pattern ON ${propertiesToString(properties)}$additionalPropertiesString${stringifyOptions(options)(expr)}"

      case DropIndexOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP INDEX ${Prettifier.escapeName(name)}$ifExistsString"

      case CreateConstraint(Variable(variable), entityName, properties, name, constraintType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        val anyAll: Boolean => String = a => if (a) "all" else "any"
        val pattern = entityName match {
          case LabelName(label)     => s"(${backtickEmpty(variable)}:${backtickEmpty(label)})"
          case RelTypeName(relType) => s"()-[${backtickEmpty(variable)}:${backtickEmpty(relType)}]-()"
          case DynamicLabelExpression(expression, all) =>
            s"(${backtickEmpty(variable)}:${anyAll(all)}$$(${expr(expression)}))"
          case DynamicRelTypeExpression(expression, all) =>
            s"()-[${backtickEmpty(variable)}:${anyAll(all)}$$(${expr(expression)})]-()"
        }
        s"${startOfCommand}FOR $pattern REQUIRE ${propertiesToString(properties)} ${constraintType.predicate}${stringifyOptions(options)(expr)}"

      case DropConstraintOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP CONSTRAINT ${Prettifier.escapeName(name)}$ifExistsString"

      case AlterCurrentGraphType(graphType, operation, _) =>
        s"ALTER CURRENT GRAPH TYPE ${operation.name()} ${GraphTypeStringifier.apply(graphType)}"
    }
    useString + commandString
  }

  def asString(adminCommand: AdministrationCommand): String = {
    val useString = asString(adminCommand.useGraph)

    def showClausesAsString(yieldOrWhere: YieldOrWhere): (String, String) = {
      val ind: IndentingQueryPrettifier = base.indented()
      yieldOrWhere match {
        case Some(Left((y, r))) =>
          (NL + ind.asString(y), r.map(ind.asString).map(NL + _).getOrElse(""))
        case Some(Right(w)) => (NL + ind.asString(w), "")
        case None           => ("", "")
      }
    }

    def getAccessString(access: Access): String = {
      val accessValue = access match {
        case ReadOnlyAccess  => "READ ONLY"
        case ReadWriteAccess => "READ WRITE"
      }
      " SET ACCESS " + accessValue
    }

    val commandString = adminCommand match {

      // User commands

      case x @ ShowUsers(yields, withAuth, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val auth = if (withAuth) " WITH AUTH" else ""
        s"${x.name}$auth$y$r"

      case x @ ShowCurrentUser(yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$y$r"

      case x @ CreateUser(userName, userOptions, ifExistsDo, externalAuths, nativeAuth, tags) =>
        val userNameString = Prettifier.escapeName(userName)
        val ifNotExists = ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => " IF NOT EXISTS"
          case _                                         => ""
        }
        val (oldStyleNativeAuthString, setAuthNativeString) = nativeAuth.map {
          auth =>
            val setPasswordString = if (auth.password.get.isEncrypted) "SET ENCRYPTED PASSWORD" else "SET PASSWORD"
            val password = expr.escapePassword(auth.password.get.password)
            val changeRequired = s"CHANGE ${if (auth.changeRequired.getOrElse(true)) "" else "NOT "}REQUIRED"
            if (x.usesOldStyleNativeAuth)
              (s" $setPasswordString $password $changeRequired", "")
            else {
              val ind: IndentingQueryPrettifier = base.indented()
              ("", ind.getNativeAuthAsString(s"$setPasswordString $password", s"SET PASSWORD $changeRequired"))
            }
        }.getOrElse(("", ""))
        val statusString =
          if (userOptions.suspended.isDefined)
            s" SET STATUS ${if (userOptions.suspended.get) "SUSPENDED" else "ACTIVE"}"
          else ""
        val homeDatabaseString = userOptions.homeDatabase.map {
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeDatabaseName(name)}"
          case _                           => None
        }.getOrElse("")

        val externalAuthString = externalAuths.sortBy(_.provider).map { auth =>
          val ind: IndentingQueryPrettifier = base.indented()
          ind.asString(auth)
        }.mkString

        val tagsString = tags.map(t => s" ${userTagsActionAsString(t)(expr)}").getOrElse("")

        s"${x.name} $userNameString$ifNotExists$oldStyleNativeAuthString$statusString$homeDatabaseString$setAuthNativeString$externalAuthString$tagsString"

      case x @ RenameUser(fromUserName, toUserName, ifExists) =>
        Prettifier.prettifyRename(x.name, fromUserName, toUserName, ifExists)

      case x @ DropUser(userName, ifExists) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeName(userName)}$ifExistsString"

      case x @ AlterUser(userName, userOptions, ifExists, externalAuths, nativeAuth, removeAuth, tags) =>
        val userNameString = Prettifier.escapeName(userName)
        val ifExistsString = if (ifExists) " IF EXISTS" else ""

        val removeAuthString = {
          if (removeAuth.all) " REMOVE ALL AUTH PROVIDERS"
          else if (!removeAuth.isEmpty)
            removeAuth.auths.map(expr(_)).mkString(" REMOVE AUTH PROVIDERS ", " REMOVE AUTH PROVIDERS ", "")
          else ""
        }

        val (oldStyleNativeAuthString, setAuthNativeString) = nativeAuth.map {
          auth =>
            val maybeChangeString =
              auth.changeRequired.map(change => s" CHANGE ${if (!change) "NOT " else ""}REQUIRED")
            (auth.password, maybeChangeString) match {
              case (Some(password), changeString) =>
                val setPasswordString = s"SET ${if (password.isEncrypted) "ENCRYPTED " else ""}PASSWORD"
                val passwordString = expr.escapePassword(password.password)
                val passwordClauseString = s"$setPasswordString $passwordString"

                if (x.usesOldStyleNativeAuth) (s" $passwordClauseString${changeString.getOrElse("")}", "")
                else {
                  val ind: IndentingQueryPrettifier = base.indented()
                  val authString =
                    if (changeString.nonEmpty)
                      ind.getNativeAuthAsString(passwordClauseString, s"SET PASSWORD${changeString.get}")
                    else ind.getNativeAuthAsString(passwordClauseString)
                  ("", authString)
                }

              case (None, Some(changeString)) =>
                if (x.usesOldStyleNativeAuth) (s" SET PASSWORD$changeString", "")
                else {
                  val ind: IndentingQueryPrettifier = base.indented()
                  ("", ind.getNativeAuthAsString(s"SET PASSWORD$changeString"))
                }
              case _ =>
                // Should not get here as we have a native auth, but lets treat it as the orElse case
                ("", "")
            }
        }.getOrElse(("", ""))

        val statusString =
          if (userOptions.suspended.isDefined)
            s" SET STATUS ${if (userOptions.suspended.get) "SUSPENDED" else "ACTIVE"}"
          else ""

        val removeHomeDatabase = userOptions.homeDatabase.collectFirst {
          case RemoveHomeDatabaseAction => " REMOVE HOME DATABASE"
        }.getOrElse("")
        val setHomeDatabaseString = userOptions.homeDatabase.collectFirst {
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeDatabaseName(name)}"
        }.getOrElse("")

        val externalAuthString = externalAuths.sortBy(_.provider).map { auth =>
          val ind: IndentingQueryPrettifier = base.indented()
          ind.asString(auth)
        }.mkString

        // CIP-254: canonical order is REMOVE* ADD* SET* (matching grammar); within SET, tags come last
        val removeTagsString = tags.collect {
          case t: RemoveTags    => t
          case t: RemoveAllTags => t
        }.map(t => s" ${userTagsActionAsString(t)(expr)}").mkString
        val addTagsString = tags.collect { case t: AddTags => t }
          .map(t => s" ${userTagsActionAsString(t)(expr)}").mkString
        val setTagsString = tags.collect { case t: SetTags => t }
          .map(t => s" ${userTagsActionAsString(t)(expr)}").mkString

        s"${x.name} $userNameString$ifExistsString$removeHomeDatabase$removeAuthString$removeTagsString$addTagsString$oldStyleNativeAuthString$statusString$setHomeDatabaseString$setAuthNativeString$externalAuthString$setTagsString"

      case x @ AlterUsers(userNames, ifExists, tags) =>
        val names = userNames.map(Prettifier.escapeName).mkString(", ")
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        val tagsString = tags.map(t => s" ${userTagsActionAsString(t)(expr)}").mkString
        s"${x.name} $names$ifExistsString$tagsString"

      case x @ SetOwnPassword(newPassword, currentPassword) =>
        s"${x.name} FROM ${expr.escapePassword(currentPassword)} TO ${expr.escapePassword(newPassword)}"

      // Auth rule commands

      case x @ ShowAuthRules(yields, _, asCommands) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val asCmd = if (asCommands) " AS COMMANDS" else ""
        s"${x.name}$asCmd$y$r"

      case x @ CreateAuthRule(authRuleName, ifExistsDo, setClauses) =>
        val setClausesString = authRuleSetClausesToString(setClauses)(expr)
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${Prettifier.escapeName(authRuleName)}$ifExists $setClausesString"

      case x @ AlterAuthRule(authRuleName, ifExists, setClauses) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeName(authRuleName)}$ifExistsString ${authRuleSetClausesToString(setClauses)(expr)}"

      case x @ RenameAuthRule(fromAuthRuleName, toAuthRuleName, ifExists) =>
        Prettifier.prettifyRename(x.name, fromAuthRuleName, toAuthRuleName, ifExists)

      case x @ DropAuthRule(ruleName, ifExists) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeName(ruleName)}$ifExistsString"

      // Role commands

      case x @ ShowRoles(withUsers, withAuthRules, _, asCommands, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val asCommandString = if (asCommands) " AS COMMANDS" else ""
        val withUsersString = if (withUsers) " WITH USERS" else ""
        val withAuthRulesString = if (withAuthRules) " WITH AUTH RULES" else ""
        s"${x.name}$withUsersString$withAuthRulesString$asCommandString$y$r"

      case x @ CreateRole(roleName, _, fromRole, ifExistsDo) =>
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        val asCopyOf = fromRole.map(r => s" AS COPY OF ${Prettifier.escapeName(r)}").getOrElse("")
        s"${x.name} ${Prettifier.escapeName(roleName)}$ifExists$asCopyOf"

      case x @ RenameRole(fromRoleName, toRoleName, ifExists) =>
        Prettifier.prettifyRename(x.name, fromRoleName, toRoleName, ifExists)

      case x @ DropRole(roleName, ifExists) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeName(roleName)}$ifExistsString"

      case x @ GrantRolesToUsers(roleNames, userNames) =>
        val start = if (roleNames.length > 1) s"${x.name}S" else x.name
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case _ @GrantRolesToAuthRules(roleNames, ruleNames) =>
        val start = if (roleNames.length > 1) "GRANT ROLES" else "GRANT ROLE"
        val authRules = if (ruleNames.length > 1) "AUTH RULES" else "AUTH RULE"
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} TO $authRules ${ruleNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ RevokeRolesFromUsers(roleNames, userNames) =>
        val start = if (roleNames.length > 1) s"${x.name}S" else x.name
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case _ @RevokeRolesFromAuthRules(roleNames, ruleNames) =>
        val start = if (roleNames.length > 1) "REVOKE ROLES" else "REVOKE ROLE"
        val authRules = if (ruleNames.length > 1) "AUTH RULES" else "AUTH RULE"
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} FROM $authRules ${ruleNames.map(Prettifier.escapeName).mkString(", ")}"

      // Privilege commands
      // dbms privileges

      case x @ GrantPrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers, expr)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers, expr)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames, _) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers, expr)} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

      // cypher 5 alter database privileges on *
      // these have AST like ON DATABASE * but should be prettified to ON DBMS (in Cypher 5)

      case x @ GrantPrivilege(
          DatabasePrivilege(privilege: DatabaseAndDbmsAction, AllDatabasesScope()),
          _,
          _,
          _,
          roleNames
        )
        if privilege.useCypher5 =>
        s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(
          DatabasePrivilege(privilege: DatabaseAndDbmsAction, AllDatabasesScope()),
          _,
          _,
          _,
          roleNames
        )
        if privilege.useCypher5 =>
        s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(
          DatabasePrivilege(privilege: DatabaseAndDbmsAction, AllDatabasesScope()),
          _,
          _,
          _,
          roleNames,
          _
        )
        if privilege.useCypher5 =>
        s"${x.name} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

      // database privileges

      case x @ GrantPrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames, expr)

      case x @ DenyPrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames, expr)

      case x @ RevokePrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames, _) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "FROM", roleNames, expr)

      // graph privileges

      case x @ GrantPrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier, expr)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ DenyPrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier, expr)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ RevokePrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames, _) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier, expr)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "FROM", roleNames)

      // load privileges

      case x @ GrantPrivilege(_: LoadPrivilege, _, _, qualifiers, roleNames) =>
        s"${x.name} ON ${Prettifier.prettifyLoadPrivilegeQualifier(expr)(qualifiers)} TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(_: LoadPrivilege, _, _, qualifiers, roleNames) =>
        s"${x.name} ON ${Prettifier.prettifyLoadPrivilegeQualifier(expr)(qualifiers)} TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(_: LoadPrivilege, _, _, qualifiers, roleNames, _) =>
        s"${x.name} ON ${Prettifier.prettifyLoadPrivilegeQualifier(expr)(qualifiers)} FROM ${Prettifier.escapeNames(roleNames)}"

      // show privileges

      case ShowSupportedPrivilegeCommand(yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"SHOW SUPPORTED PRIVILEGES$y$r"

      case ShowPrivileges(scope, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES$y$r"

      case ShowPrivilegeCommands(scope, asRevoke, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val asCommand = if (asRevoke) " AS REVOKE COMMANDS" else " AS COMMANDS"
        s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES$asCommand$y$r"

      // Database commands

      case x @ CreateDatabase(
          dbName,
          ifExistsDo,
          options,
          waitUntilComplete,
          topology,
          defaultCypherVersion,
          shardDef
        ) =>
        val formattedOptions = stringifyOptions(options)(expr)
        val maybeTopologyString = topology.map(Prettifier.extractTopology).getOrElse("")
        val maybeShardString = shardDef.map(Prettifier.extractShardDefinition).getOrElse("")
        val maybeCypherVersion = defaultCypherVersion.map(cv => s" DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}$ifExists$maybeCypherVersion$maybeTopologyString$maybeShardString$formattedOptions${waitUntilComplete.name}"

      case x @ CreateReplicaDatabase(dbName, ifExistsDo, options, waitUntilComplete, topology, defaultCypherVersion) =>
        val formattedOptions = stringifyOptions(options)(expr)
        val maybeTopologyString = topology.map(Prettifier.extractTopology).getOrElse("")
        val maybeCypherVersion = defaultCypherVersion.map(cv => s" DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val ifExists = ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}$ifExists$maybeCypherVersion$maybeTopologyString$formattedOptions${waitUntilComplete.name}"

      case x @ CreateCompositeDatabase(name, ifExistsDo, options, waitUntilComplete, defaultCypherVersion) =>
        val formattedOptions = stringifyOptions(options)(expr)
        val maybeCypherVersion = defaultCypherVersion.map(cv => s" DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${escapeDatabaseName(name)}$ifExists$maybeCypherVersion$formattedOptions${waitUntilComplete.name}"

      case x @ DropDatabase(dbName, ifExists, _, aliasAction, additionalAction, waitUntilComplete) =>
        val maybeIfExists = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}$maybeIfExists ${aliasAction.name} ${additionalAction.name}${waitUntilComplete.name}"

      case x @ AlterDatabase(
          dbName,
          ifExists,
          access,
          topology,
          options,
          optionsToRemove,
          waitUntilComplete,
          defaultCypherVersion,
          shardDefinition,
          replicas
        ) =>
        val maybeAccessString = access.map(getAccessString).getOrElse("")
        val maybeIfExists = if (ifExists) " IF EXISTS" else ""
        val maybeTopologyString = topology
          .map(topo => s" SET${Prettifier.extractTopology(topo)}").getOrElse(replicas
            .map(topo => s" SET${Prettifier.extractShardTopology(Some(topo))}").getOrElse(""))
        val maybeCypherVersion =
          defaultCypherVersion.map(cv => s" SET DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val formattedOptions = asIndividualOptions(options)
        val formattedOptionsToRemove = optionsToRemove.map(o => s" REMOVE OPTION ${backtickEmpty(o)}").mkString("")
        val maybeShards = shardDefinition.map(s => Prettifier.extractAlterShardDefinition(s)).getOrElse("")
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}$maybeIfExists$maybeAccessString$maybeTopologyString$maybeShards$formattedOptions$formattedOptionsToRemove$maybeCypherVersion${waitUntilComplete.name}"

      case x @ StartDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}${waitUntilComplete.name}"

      case x @ StopDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeDatabaseName(dbName)}${waitUntilComplete.name}"

      // Alias commands

      case x @ CreateLocalDatabaseAlias(aliasName, targetName, ifExistsDo, properties) =>
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${Prettifier.escapeDatabaseName(aliasName)}$ifExists FOR DATABASE ${Prettifier.escapeDatabaseName(targetName)}$propertiesString"

      case x @ CreateRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExistsDo,
          url,
          remoteAliasCredentials,
          driverSettings,
          properties,
          defaultLanguage
        ) =>
        val urlString = url match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }

        val driverSettingsString = propertiesMapToString("DRIVER", driverSettings)
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        val defaultLanguageString = defaultLanguage.map(cv => s" DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val credentials = remoteAliasCredentials match {
          case RemoteAliasStoredCredentials(username, password) =>
            s"USER ${Prettifier.escapeName(username)} PASSWORD ${expr.escapePassword(password)}"
          case OidcCredentialForwarding() => "OIDC CREDENTIAL FORWARDING"
        }

        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${Prettifier.escapeDatabaseName(aliasName)}$ifExists FOR DATABASE ${Prettifier.escapeDatabaseName(targetName)} AT $urlString " +
          credentials + driverSettingsString + defaultLanguageString + propertiesString

      case x @ DropDatabaseAlias(aliasName, ifExists) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeDatabaseName(aliasName)}$ifExistsString FOR DATABASE"

      case x @ AlterLocalDatabaseAlias(aliasName, targetName, ifExists, properties) =>
        val target = targetName.map(tgt => "TARGET " + Prettifier.escapeDatabaseName(tgt)).getOrElse("")
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeDatabaseName(aliasName)}$ifExistsString SET DATABASE $target$propertiesString"

      case x @ AlterRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExists,
          url,
          username,
          password,
          driverSettings,
          properties,
          defaultLanguage
        ) =>
        val targetString = targetName match {
          case Some(targetName) =>
            val urlString = url match {
              case Some(Left(s))          => s" AT ${expr.quote(s)}"
              case Some(Right(parameter)) => s" AT ${expr(parameter)}"
              case _                      => ""
            }
            s" TARGET ${Prettifier.escapeDatabaseName(targetName)}$urlString"
          case None => ""
        }

        val userString = username match {
          case Some(username) =>
            s" USER ${Prettifier.escapeName(username)}"
          case None => ""
        }

        val passwordString = password match {
          case Some(password) =>
            s" PASSWORD ${expr.escapePassword(password)}"
          case None => ""
        }

        val driverSettingsString = propertiesMapToString("DRIVER", driverSettings)
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        val defaultLanguageString = defaultLanguage.map(cv => s" DEFAULT LANGUAGE ${cv.description}").getOrElse("")
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"${x.name} ${Prettifier.escapeDatabaseName(aliasName)}$ifExistsString SET DATABASE$targetString$userString$passwordString$driverSettingsString$defaultLanguageString$propertiesString"

      case x @ ShowAliases(aliasName, yields, _) =>
        val an = aliasName.map(an => s" ${escapeDatabaseName(an)}").getOrElse("")
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$an FOR DATABASE$y$r"

      // Server commands

      case x @ EnableServer(serverName, options) =>
        val name = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        val optionString = stringifyOptions(options)(expr)
        s"${x.name} $name$optionString"

      case x @ AlterServer(serverName, options) =>
        val name = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        val optionString = stringifyOptions(options)(expr)
        s"${x.name} $name SET$optionString"

      case x @ RenameServer(serverName, newName) =>
        val from = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        val to = newName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        s"${x.name} $from TO $to"

      case x @ DropServer(serverName) =>
        val name = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        s"${x.name} $name"

      case x @ ShowServers(yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$y$r"

      case x @ DeallocateServers(dryRun, serverNames) =>
        val dryRunString = if (dryRun) "DRYRUN " else ""
        val commandString = if (serverNames.length > 1) s"${x.name}S" else x.name
        val names = serverNames.map {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        s"$dryRunString$commandString ${names.mkString(", ")}"

      case x @ ReallocateDatabases(dryRun) =>
        val dryRunString = if (dryRun) "DRYRUN " else ""
        s"$dryRunString${x.name}"

      case command => throw new InternalError(s"Unexpected command $command")
    }
    useString + commandString
  }

  private def asString(use: Option[GraphSelection]) = {
    use.filter(_ => useInCommands).map(u => base.dispatch(u) + NL).getOrElse("")
  }

  private def asIndividualOptions(options: Options) = options match {
    case NoOptions => ""
    case OptionsMap(map) => map.map {
        case (key, value) => s" SET OPTION ${backtickEmpty(key)} ${expr(value)}"
      }.mkString("")
    case OptionsParam(_) => throw new InternalError("Expected NoOptions or OptionsMap but was OptionsParam")
  }

  case class IndentingQueryPrettifier(indentLevel: Int = 0) extends Prettifier.QueryPrettifier {
    def indented(): IndentingQueryPrettifier = copy(indentLevel + 1)
    val INDENT: String = BASE_INDENT * indentLevel

    private def asNewLine(l: String): String = NL + l

    private def appendSpaceIfNonEmpty(s: String): String = if (s.nonEmpty) s"$s " else s

    def query(q: Query): String =
      q match {
        case SingleQuery(clauses) =>
          // Need to filter away empty strings as SHOW/TERMINATE commands might get an empty string from YIELD/WITH/RETURN clauses
          clauses.map(dispatch).filter(_.nonEmpty).mkString(NL)

        case union: Union =>
          val lhs = query(union.lhs)
          val rhs = query(union.rhs)
          val operation = union match {
            case _: UnionAll | _: ProjectingUnionAll           => s"${INDENT}UNION ALL"
            case _: UnionDistinct | _: ProjectingUnionDistinct => s"${INDENT}UNION"
          }
          Seq(lhs, operation, rhs).mkString(NL)
        case TopLevelBraces(innerQuery, use) =>
          val useStr = use.map(asString(_) ++ " ").getOrElse("")
          useStr ++ Seq(s"$INDENT{", indented().query(innerQuery), s"$INDENT}").mkString(NL)
        case ConditionalQueryWhen(branches, default) =>
          (branches.map(b =>
            s"${INDENT}WHEN ${b.predicate.fold("N/A")(expr(_, shouldBacktickEmpty = true))} THEN ${query(b.query).trim}"
          ) ++
            default.map(d => s"${INDENT}ELSE ${query(d.query).trim}")).mkString(NL)
        case NextStatement(queries) =>
          queries.map(query).mkString(s"$NL$NL${INDENT}NEXT$NL$NL")
        case QueryWithLocalDefinitions(definitions, q) =>
          val defs = definitions.map(asString).mkString(NL)
          s"$defs$NL$NL${query(q)}"
      }

    def asString(lfs: LocalFieldSignature): String = {
      val defaultStr = lfs.default.map(d => s" = ${expr(d, shouldBacktickEmpty = true)}").getOrElse("")
      val typeStr = lfs.typ.map(t => s" :: ${t.description}").getOrElse("")
      s"${lfs.name}$typeStr$defaultStr"
    }

    def asString(lcd: LocalCallableDefinition): String = {
      val ldStr = lcd match {
        case LocalProcedureDefinition(name, inputSignature, outputSignature, procedureBody) =>
          val procedureName = expr(name, shouldBacktickEmpty = true)
          val in = inputSignature.map(asString).mkString("(", ", ", ")")
          val out = outputSignature.map(_.map(asString).mkString(" :: (", ", ", ")")).getOrElse("")
          val body = s"{$NL${indented().query(procedureBody)}$NL$INDENT}"
          s"PROCEDURE $procedureName$in$out $body"
        case LocalFunctionDefinition(name, inputSignature, outputSignature, functionBody) =>
          val functionName = expr(name, shouldBacktickEmpty = true)
          val in = inputSignature.map(asString).mkString("(", ", ", ")")
          val out = outputSignature.map(t => s" :: ${t.description}").getOrElse("")
          val body = functionBody match {
            case ExpressionBody(ex) => s"= ${expr(ex, shouldBacktickEmpty = true)}"
            case QueryBody(qu)      => s"{$NL${indented().query(qu)}$NL$INDENT}"
          }
          s"FUNCTION $functionName$in$out $body"
      }
      s"${INDENT}DEFINE $ldStr"
    }

    def asString(clause: Clause): String = dispatch(clause)

    def dispatch(clause: Clause): String = clause match {
      case u: UseGraph                    => asString(u)
      case e: Return                      => asString(e)
      case f: Finish                      => asString(f)
      case m: Match                       => asString(m)
      case c: ImportingWithSubqueryCall   => asString(c)
      case c: ScopeClauseSubqueryCall     => asString(c)
      case w: With                        => asString(w)
      case y: Yield                       => asString(y)
      case c: Create                      => asString(c)
      case i: Insert                      => asString(i)
      case u: Unwind                      => asString(u)
      case u: UnresolvedCall              => asString(u)
      case c: CallClause                  => asString(c.asUnresolvedCall)
      case s: ShowIndexesClause           => asString(s)
      case s: ShowConstraintsClause       => asString(s)
      case s: ShowCurrentGraphTypeClause  => asString(s)
      case s: ShowProceduresClause        => asString(s)
      case s: ShowFunctionsClause         => asString(s)
      case s: ShowTransactionsClause      => asString(s)
      case t: TerminateTransactionsClause => asString(t)
      case s: ShowSettingsClause          => asString(s)
      case s: ShowDatabasesClause         => asString(s)
      case s: SetClause                   => asString(s)
      case r: Remove                      => asString(r)
      case d: Delete                      => asString(d)
      case m: Merge                       => asString(m)
      case l: LoadCSV                     => asString(l)
      case f: Foreach                     => asString(f)
      case c =>
        val ext = extension.asString(this)
        ext.applyOrElse(c, fallback)
    }

    private def fallback(clause: Clause): String =
      clause.asCanonicalStringVal

    def asString(u: UseGraph): String = {
      u.graphReference match {
        case GraphDirectReference(catalogName) => s"${INDENT}USE ${catalogName.asCanonicalNameString}"
        case GraphFunctionReference(functionInvocation: FunctionInvocation, _) =>
          s"${INDENT}USE ${expr(functionInvocation, shouldBacktickEmpty = true)}"
      }
    }

    def asString(m: Match): String = {
      val o = if (m.optional) "OPTIONAL " else ""
      val mm = appendSpaceIfNonEmpty(m.matchMode.prettified)
      val p = expr.patterns.apply(m.pattern)
      val ind = indented()
      val w = m.where.map(ind.asString).map(asNewLine).getOrElse("")
      val s = m.search.map(ind.asString).map(asNewLine).getOrElse("")
      val h = m.hints.map(ind.asString).map(asNewLine).mkString
      s"$INDENT${o}MATCH $mm$p$h$s$w"
    }

    def asString(c: ImportingWithSubqueryCall): String = {
      val optional = if (c.optional) "OPTIONAL " else ""
      val inTxParams = c.inTransactionsParameters.map(asString).getOrElse("")
      s"""$INDENT${optional}CALL {
         |${indented().query(c.innerQuery)}
         |$INDENT}$inTxParams""".stripMargin
    }

    def asString(c: ScopeClauseSubqueryCall): String = {
      val optional = if (c.optional) "OPTIONAL " else ""
      val inTxParams = c.inTransactionsParameters.map(asString).getOrElse("")
      s"""$INDENT${optional}CALL (${
          if (c.isImportingAll) "*"
          else c.importedVariables.map(expr(_, shouldBacktickEmpty = true)).mkString("", ",", "")
        }) {
         |${indented().query(c.innerQuery)}
         |$INDENT}$inTxParams""".stripMargin
    }

    def asString(ip: InTransactionsParameters): String = {
      val ofRows = ip.batchParams.map(_.batchSize) match {
        case Some(size) => " OF " + expr(size, shouldBacktickEmpty = true) + " ROWS"
        case None       => ""
      }
      val concurrency = ip.concurrencyParams match {
        case Some(InTransactionsConcurrencyParameters(Some(explicit))) =>
          " " + expr(explicit, shouldBacktickEmpty = true) + " CONCURRENT"
        case Some(InTransactionsConcurrencyParameters(None)) => " CONCURRENT"
        case None                                            => ""
      }
      val retryParameters = ip.errorParams.map(_.retryParameters.map(_.timeout)) match {
        case Some(Some(Some(timeout))) =>
          " " + expr(timeout, shouldBacktickEmpty = true) + " SECONDS"
        case _ => ""
      }
      val onError = ip.errorParams.map(_.behaviour) match {
        case Some(OnErrorBreak)             => s" ON ERROR BREAK"
        case Some(OnErrorContinue)          => s" ON ERROR CONTINUE"
        case Some(OnErrorFail)              => s" ON ERROR FAIL"
        case Some(OnErrorRetryThenContinue) => s" ON ERROR RETRY$retryParameters THEN CONTINUE"
        case Some(OnErrorRetryThenBreak)    => s" ON ERROR RETRY$retryParameters THEN BREAK"
        case Some(OnErrorRetryThenFail)     => s" ON ERROR RETRY$retryParameters THEN FAIL"
        case None                           => ""
      }
      val reportStatus = ip.reportParams.map(_.reportAs) match {
        case Some(statusVar) => s" REPORT STATUS AS ${backtickEmpty(statusVar.name)}"
        case None            => ""
      }
      val disjointBy = ip.disjointByParams.map(_.mode) match {
        case Some(InTransactionsDisjointByMode.DisjointByAuto) => " DISJOINT BY AUTO"
        case Some(InTransactionsDisjointByMode.DisjointByNone) => " DISJOINT BY NONE"
        case Some(InTransactionsDisjointByMode.DisjointByExpressions(expressions)) =>
          " DISJOINT BY (" + expressions.map(expr(_, shouldBacktickEmpty = true)).mkString(", ") + ")"
        case None => ""
      }
      s" IN$concurrency TRANSACTIONS$ofRows$disjointBy$onError$reportStatus"
    }

    def asString(w: Where): String =
      s"${INDENT}WHERE ${expr(w.expression, shouldBacktickEmpty = true)}"

    def asString(s: Search): String = {

      val indexName = Prettifier.escapeName(s.indexName)

      val maybeScore = if (s.score.isDefined) s" SCORE AS ${Prettifier.escapeName(s.score.get)}" else ""

      val maybeWhere = s.where.map(w => s"$INDENT${asString(w)}").map(asNewLine).getOrElse("")

      s"""${INDENT}SEARCH ${backtickEmpty(s.bindingVariable.name)} IN (
         |$INDENT${INDENT}VECTOR INDEX $indexName
         |$INDENT${INDENT}FOR ${expr(s.embedding, shouldBacktickEmpty = true)}$maybeWhere
         |$INDENT${INDENT}LIMIT ${expr(s.limit.expression, shouldBacktickEmpty = true)}
         |$INDENT)$maybeScore""".stripMargin
    }

    def asString(m: Hint): String = {
      m match {
        case UsingIndexHint(v, l, ps, s, t) => Seq(
            s"${INDENT}USING ",
            t match {
              case UsingAnyIndexType   => "INDEX "
              case UsingTextIndexType  => "TEXT INDEX "
              case UsingRangeIndexType => "RANGE INDEX "
              case UsingPointIndexType => "POINT INDEX "
            },
            if (s == SeekOnly) "SEEK " else "",
            expr(v, shouldBacktickEmpty = true),
            ":",
            expr(l, shouldBacktickEmpty = true),
            ps.map(expr(_, shouldBacktickEmpty = true)).mkString("(", ",", ")")
          ).mkString

        case UsingScanHint(v, l) => Seq(
            s"${INDENT}USING SCAN ",
            expr(v, shouldBacktickEmpty = true),
            ":",
            expr(l, shouldBacktickEmpty = true)
          ).mkString

        case UsingJoinHint(vs) => Seq(
            s"${INDENT}USING JOIN ON ",
            vs.map(expr(_, shouldBacktickEmpty = true)).toIterable.mkString(", ")
          ).mkString

        // AST-level hint.
        case UsingExpandHint(steps) =>
          val renderedSteps =
            steps.iterator.map(s => renderExpandStep(s.mode, s.from, s.to, s.via)).mkString(", ")
          s"${INDENT}USING EXPAND $renderedSteps"

        // IR-level hint. This is what will be reported on in VerifyBestPlan.
        case h: UsingExpandStepHint =>
          s"${INDENT}USING EXPAND ${renderExpandStep(h.mode, h.from, h.to, h.via)}"

        // Note: This hint cannot be written in Cypher.
        case UsingStatefulShortestPathAll(vs) => Seq(
            s"${INDENT}USING SSP_ALL ON ",
            vs.map(expr(_, shouldBacktickEmpty = true)).toIterable.mkString(", ")
          ).mkString

        // Note: This hint cannot be written in Cypher.
        case UsingStatefulShortestPathInto(vs) => Seq(
            s"${INDENT}USING SSP_INTO ON ",
            vs.map(expr(_, shouldBacktickEmpty = true)).toIterable.mkString(", ")
          ).mkString
      }
    }

    private def renderExpandMode(mode: ExpandHintMode): String = mode match {
      case ExpandHintAll  => "ALL"
      case ExpandHintInto => "INTO"
    }

    private def renderExpandStep(
      mode: Option[ExpandHintMode],
      from: Option[Variable],
      to: Option[Variable],
      via: Option[Variable]
    ): String = {
      val modeStr = mode.map(renderExpandMode)
      val endpoints = (from, to) match {
        case (Some(f), Some(t)) =>
          Some(s"FROM ${expr(f, shouldBacktickEmpty = true)} TO ${expr(t, shouldBacktickEmpty = true)}")
        case (None, None) => None
        case (Some(_), None) | (None, Some(_)) =>
          throw new IllegalStateException(
            "ExpandStep with partial endpoint specification (only one of FROM/TO set) — grammar/semantic check should prevent this"
          )
      }
      val viaStr = via.map(v => s"VIA ${expr(v, shouldBacktickEmpty = true)}")
      Seq(modeStr, endpoints, viaStr).flatten.mkString(" ")
    }

    def asString(ma: MergeAction): String = ma match {
      case OnMatch(set)  => s"${INDENT}ON MATCH ${asString(set)}"
      case OnCreate(set) => s"${INDENT}ON CREATE ${asString(set)}"
    }

    def asString(m: Merge): String = {
      val p = expr.patterns.apply(m.pattern)
      val ind = indented()
      val a = m.actions.map(ind.asString).map(asNewLine).mkString
      s"${INDENT}MERGE $p$a"
    }

    def asString(o: Skip): String =
      s"$INDENT${o.name} ${expr(o.expression, shouldBacktickEmpty = true)}"

    def asString(o: Limit): String =
      s"${INDENT}LIMIT ${expr(o.expression, shouldBacktickEmpty = true)}"

    def asString(g: GroupBy): String = s"${INDENT}GROUP BY " + {
      g.groupingElements match {
        case ExplicitGroupingElements(elements) =>
          elements.map(elem => expr(elem, shouldBacktickEmpty = true)).mkString(", ")
        case GroupingAll()  => "ALL"
        case GroupingNone() => "()"
      }
    }

    def asString(o: OrderBy): String = s"${INDENT}ORDER BY " + {
      o.sortItems.map {
        case AscSortItem(expression)  => expr(expression, shouldBacktickEmpty = true) + " ASCENDING"
        case DescSortItem(expression) => expr(expression, shouldBacktickEmpty = true) + " DESCENDING"
      }.mkString(", ")
    }

    def asString(r: ReturnItem): String = r match {
      case AliasedReturnItem(e, v) =>
        expr(e, shouldBacktickEmpty = true) + " AS " + expr(v, shouldBacktickEmpty = true)
      case UnaliasedReturnItem(e, _) => expr(e, shouldBacktickEmpty = true)
    }

    def asString(r: ReturnItems): String = asString(r, shouldBacktickEmpty = true)

    def asString(r: ReturnItems, shouldBacktickEmpty: Boolean): String = {
      val as = if (r.includeExisting) Seq("*") else Seq()
      val is = r.items.map(asString)
      (as ++ is).mkString(", ")
    }

    def asString(r: Return): String =
      if (r.returnType == ReturnAddedInRewrite) ""
      else {
        val d = if (r.distinct) " DISTINCT" else ""
        val i = asString(r.returnItems)
        val ind = indented()
        val g = r.groupBy.map(ind.asString).map(asNewLine).getOrElse("")
        val o = r.orderBy.map(ind.asString).map(asNewLine).getOrElse("")
        val l = r.limit.map(ind.asString).map(asNewLine).getOrElse("")
        val s = r.skip.map(ind.asString).map(asNewLine).getOrElse("")
        s"${INDENT}RETURN$d $i$g$o$s$l"
      }

    def asString(@unused f: Finish): String = s"${INDENT}FINISH"

    def asString(w: With): String = {
      val ind = indented()
      val rewrittenClauses = List(
        w.groupBy.map(ind.asString),
        w.orderBy.map(ind.asString),
        w.skip.map(ind.asString),
        w.limit.map(ind.asString),
        w.where.map(ind.asString)
      ).flatten
      lazy val rewrittenClausesStrWithNlSeparators = rewrittenClauses.mkString(NL)

      w.withType match {
        case ParsedAsOrderBy | ParsedAsSkip | ParsedAsLimit =>
          s"$INDENT${rewrittenClausesStrWithNlSeparators.trim}"
        case ParsedAsFilter =>
          s"${INDENT}FILTER ${rewrittenClausesStrWithNlSeparators.trim}"
        case ParsedAsLet =>
          val items = w.returnItems.items.map {
            case AliasedReturnItem(e, v) =>
              expr(v, shouldBacktickEmpty = true) + " = " + expr(e, shouldBacktickEmpty = true)
            case UnaliasedReturnItem(_, _) =>
              new IllegalStateException("A With that is ParsedAsLet shall not contain UnaliasedReturnItem.")
          }.mkString(", ")
          s"${INDENT}LET $items"
        case ParsedAsYield | AddedInRewriteShowCommands =>
          // part of SHOW/TERMINATE TRANSACTION which prettifies the YIELD items part
          // but it no longer knows the subclauses, hence prettifying them here

          // only add newlines between subclauses and not in front of the first one
          if (rewrittenClauses.nonEmpty) s"$INDENT$rewrittenClausesStrWithNlSeparators"
          else ""
        case _ =>
          val d = if (w.distinct) " DISTINCT" else ""
          val i = asString(w.returnItems)

          s"${INDENT}WITH$d $i${rewrittenClauses.map(asNewLine).mkString}"
      }
    }

    def asString(y: Yield): String = {
      val i = asString(y.returnItems)
      val ind = indented()
      val o = y.orderBy.map(ind.asString).map(asNewLine).getOrElse("")
      val l = y.limit.map(ind.asString).map(asNewLine).getOrElse("")
      val s = y.skip.map(ind.asString).map(asNewLine).getOrElse("")
      val wh = y.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}YIELD $i$o$s$l$wh"
    }

    def asString(c: Create): String = {
      val p = expr.patterns.apply(c.pattern)
      s"${INDENT}CREATE $p"
    }

    def asString(i: Insert): String = {
      val p = expr.patterns.apply(i.pattern)
      s"${INDENT}INSERT $p"
    }

    def asString(u: Unwind): String = {
      if (u.useForInSyntax) {
        s"${INDENT}FOR ${expr(u.variable, shouldBacktickEmpty = true)} IN ${expr(u.expression, shouldBacktickEmpty = true)}"
      } else {
        s"${INDENT}UNWIND ${expr(u.expression, shouldBacktickEmpty = true)} AS ${expr(u.variable, shouldBacktickEmpty = true)}"
      }
    }

    def asString(u: UnresolvedCall): String = {
      val name = expr(u.procedureName, shouldBacktickEmpty = true)
      val optional = if (u.optional) "OPTIONAL " else ""
      val args = u.declaredArguments.map(_.filter {
        case CoerceTo(_: ImplicitProcedureArgument, _) => false
        case _: ImplicitProcedureArgument              => false
        case _                                         => true
      })
      val arguments =
        args.map(list => list.map(expr(_, shouldBacktickEmpty = true)).mkString("(", ", ", ")")).getOrElse("")
      val ind = indented()
      val yields =
        if (u.yieldAll) asNewLine(s"${indented().INDENT}YIELD *")
        else u.declaredResult.filter(_.items.nonEmpty).map(ind.asString).map(asNewLine).getOrElse("")
      s"$INDENT${optional}CALL $name$arguments$yields"
    }

    def asString(r: ProcedureResult): String = {
      def item(i: ProcedureResultItem) =
        i.output.map(expr(_, shouldBacktickEmpty = true) + " AS ").getOrElse("") + expr(
          i.variable,
          shouldBacktickEmpty = true
        )
      val items = r.items.map(item).mkString(", ")
      val ind = indented()
      val where = r.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}YIELD $items$where"
    }

    def asString(s: ShowIndexesClause): String = {
      val indexType = s.indexType.prettyPrint
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"${INDENT}SHOW $indexType INDEXES$where$yielded"
    }

    def asString(s: ShowConstraintsClause): String = {
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"${INDENT}SHOW ${s.constraintType.prettyPrint} CONSTRAINTS$where$yielded"
    }

    def asString(s: ShowCurrentGraphTypeClause): String = {
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"$INDENT${s.name}$where$yielded"
    }

    def asString(s: ShowProceduresClause): String = {
      val executable = getExecutablePart(s.executable)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"$INDENT${s.name}$executable$where$yielded"
    }

    def asString(s: ShowFunctionsClause): String = {
      val functionType = s.functionType.prettyPrint
      val executable = getExecutablePart(s.executable)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"${INDENT}SHOW $functionType FUNCTIONS$executable$where$yielded"
    }

    private def getExecutablePart(executable: Option[ExecutableBy]): String = executable match {
      case Some(CurrentUser) => " EXECUTABLE BY CURRENT USER"
      case Some(User(name))  => s" EXECUTABLE BY ${backtickEmpty(name)}"
      case None              => ""
    }

    def asString(s: ShowTransactionsClause): String = {
      val ids = namesAsString(s.names)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"${INDENT}SHOW TRANSACTIONS$ids$where$yielded"
    }

    def asString(s: TerminateTransactionsClause): String = {
      val ids = namesAsString(s.names)
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"${INDENT}TERMINATE TRANSACTIONS$ids$yielded"
    }

    def asString(s: ShowSettingsClause): String = {
      val names = namesAsString(s.names)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"$INDENT${s.name}$names$where$yielded"
    }

    def asString(s: ShowDatabasesClause): String = {
      val ind = indented()
      val optionalName = s.dbScope match {
        case SingleNamedDatabaseScope(dbName) => s" ${Prettifier.escapeDatabaseName(dbName)}"
        case _                                => ""
      }
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = yieldAsString(s.yieldItems, s.yieldAll, s.yieldWith)
      s"$INDENT${s.name}$optionalName$where$yielded"
    }

    private def namesAsString(ids: CommandClauseNames): String = ids match {
      case NoNames => ""
      case CommaSeparatedNames(l) =>
        l.expressions.map(id => expr(id, shouldBacktickEmpty = true)).mkString(" ", ", ", "")
      case ExpressionNames(e) => s" ${expr(e, shouldBacktickEmpty = true)}"
    }

    private def yieldAsString(
      yieldItems: List[CommandResultItem],
      yieldAll: Boolean,
      yieldWith: Option[With]
    ): String = {
      val yieldPart = if (yieldItems.nonEmpty) {
        val items = yieldItems.map(c => {
          if (!c.aliasedVariable.name.equals(c.originalName)) {
            backtickEmpty(c.originalName) + " AS " + expr(c.aliasedVariable, shouldBacktickEmpty = true)
          } else expr(c.aliasedVariable, shouldBacktickEmpty = true)
        }).mkString(", ")
        asNewLine(s"${INDENT}YIELD $items")
      } else if (yieldAll) asNewLine(s"${INDENT}YIELD *")
      else ""
      val extraClauses =
        yieldWith.map(asString).filter(_.nonEmpty).map(asNewLine).getOrElse("")
      yieldPart + extraClauses
    }

    def asString(s: SetClause): String = {
      s"${INDENT}SET ${prettifySetItems(s.items)}"
    }

    def asString(r: Remove): String = {
      s"${INDENT}REMOVE ${prettifyRemoveItems(r.items)}"
    }

    def asString(v: LoadCSV): String = {
      val withHeaders = if (v.withHeaders) " WITH HEADERS" else ""
      val url = expr(v.urlString, shouldBacktickEmpty = true)
      val varName = expr(v.variable, shouldBacktickEmpty = true)
      val fieldTerminator =
        v.fieldTerminator.map(x => " FIELDTERMINATOR " + expr(x, shouldBacktickEmpty = true)).getOrElse("")
      s"${INDENT}LOAD CSV$withHeaders FROM $url AS $varName$fieldTerminator"
    }

    def asString(delete: Delete): String = {
      val detach = if (delete.forced) "DETACH " else ""
      s"$INDENT${detach}DELETE ${delete.expressions.map(expr(_, shouldBacktickEmpty = true)).mkString(", ")}"
    }

    def asString(foreach: Foreach): String = {
      val varName = expr(foreach.variable, shouldBacktickEmpty = true)
      val list = expr(foreach.expression, shouldBacktickEmpty = true)
      val updates = foreach.updates.map(dispatch).mkString(s"$NL  ", s"$NL  ", NL)
      s"${INDENT}FOREACH ( $varName IN $list |$updates)"
    }

    def asString(auth: ExternalAuth): String = {
      val idString: String = expr(auth.id)
      authAsString(auth.provider, s"SET ID $idString")
    }

    def getNativeAuthAsString(passwordClauses: String*): String = {
      authAsString(NATIVE_AUTH, passwordClauses: _*)
    }

    private def authAsString(provider: String, innerClauses: String*): String = {
      val providerString = expr.quote(provider)
      val innerClausesString: String = innerClauses.mkString(s"$NL$INDENT$BASE_INDENT")
      s"$NL${INDENT}SET AUTH PROVIDER $providerString {$NL$INDENT$BASE_INDENT$innerClausesString$NL$INDENT}"
    }
  }
}

object Prettifier {

  // In a lot of cases, we use multi-line strings to construct our line-breaks. Let's make sure we stay consistent with that here.
  val NL: String =
    """
      |""".stripMargin
  val BASE_INDENT: String = "  "

  trait QueryPrettifier {
    def INDENT: String
    def asString(clause: Clause): String
  }

  trait ClausePrettifier {
    def asString(ctx: QueryPrettifier): PartialFunction[Clause, String]
  }

  // Needs to be non-private for apoc
  // noinspection ScalaWeakerAccess
  object EmptyExtension extends ClausePrettifier {
    def asString(ctx: QueryPrettifier): PartialFunction[Clause, String] = PartialFunction.empty
  }

  private def prettifyRename(
    commandName: String,
    fromName: Expression,
    toName: Expression,
    ifExists: Boolean
  ): String = {
    val maybeIfExists = if (ifExists) " IF EXISTS" else ""
    s"$commandName ${escapeName(fromName)}$maybeIfExists TO ${escapeName(toName)}"
  }

  private def extractScope(scope: ShowPrivilegeScope): String = {
    scope match {
      case ShowUserPrivileges(name) =>
        if (name.isDefined)
          s"USER ${escapeName(name.get)}"
        else
          "USER"
      case ShowUsersPrivileges(names) =>
        if (names.size == 1)
          s"USER ${escapeName(names.head)}"
        else
          s"USERS ${escapeNames(names)}"
      case ShowRolesPrivileges(names) =>
        if (names.size == 1)
          s"ROLE ${escapeName(names.head)}"
        else
          s"ROLES ${escapeNames(names)}"
      case ShowAllPrivileges() => "ALL"
    }
  }

  private def prettifyDatabasePrivilege(
    privilegeName: String,
    dbScope: DatabaseScope,
    qualifier: List[PrivilegeQualifier],
    preposition: String,
    roleNames: Seq[Expression],
    expr: ExpressionStringifier
  ): String = {
    val (dbName, home, multiple) = Prettifier.extractDbScope(dbScope)
    val db =
      if (home) {
        s"$dbName DATABASE"
      } else if (multiple) {
        s"DATABASES $dbName"
      } else {
        s"DATABASE $dbName"
      }
    s"$privilegeName${extractQualifierString(qualifier, expr)} ON $db $preposition ${escapeNames(roleNames)}"
  }

  private def prettifyGraphPrivilege(
    privilegeName: String,
    graphScope: GraphScope,
    qualifierString: String,
    resource: Option[ActionResourceBase],
    preposition: String,
    roleNames: Seq[Expression]
  ): String = {

    val resourceName = resource match {
      case Some(PropertyResource(name))    => s" {${backtickEmpty(name)}}"
      case Some(PropertiesResource(names)) => s" {${names.map(backtickEmpty).mkString(", ")}}"
      case Some(AllPropertyResource())     => " {*}"
      case Some(LabelResource(name))       => s" ${backtickEmpty(name)}"
      case Some(LabelsResource(names))     => s" ${names.map(backtickEmpty).mkString(", ")}"
      case Some(AllLabelResource())        => " *"
      case None                            => ""
      case _                               => throw new IllegalStateException(s"Unknown resource: $resource")
    }
    val scope = s"${extractGraphScope(graphScope)}"
    s"$privilegeName$resourceName ON $scope$qualifierString $preposition ${Prettifier.escapeNames(roleNames)}"
  }

  private def prettifyLoadPrivilegeQualifier(
    expr: ExpressionStringifier
  ): PartialFunction[List[PrivilegeQualifier], String] = {
    case LoadAllQualifier() :: Nil                  => s"ALL DATA"
    case LoadUrlQualifier(Left(urlString)) :: Nil   => s"URL ${expr.quote(urlString)}"
    case LoadUrlQualifier(Right(urlParam)) :: Nil   => s"URL ${expr(urlParam)}"
    case LoadCidrQualifier(Left(cidrString)) :: Nil => s"CIDR ${expr.quote(cidrString)}"
    case LoadCidrQualifier(Right(cidrParam)) :: Nil => s"CIDR ${expr(cidrParam)}"
  }

  private def prettifyGraphQualifier(
    action: GraphAction,
    qualifier: List[PrivilegeQualifier],
    expr: ExpressionStringifier
  ): String = {
    // For WRITE, we don't want to print out the qualifier. For SET and REMOVE LABEL, it is printed out in another position.
    if (action.name.equals("WRITE") || action.name.equals("SET LABEL") || action.name.equals("REMOVE LABEL")) {
      ""
    } else {
      extractQualifierString(qualifier, expr)
    }
  }

  private def extractQualifierPart(qualifier: List[PrivilegeQualifier], expr: ExpressionStringifier): Option[String] = {
    def stringifyQualifiedName(glob: String) = {
      // If we have multiple . in a row, just escape the whole thing to not loose any of them
      // or risk breaking parsing of the prettified string, as multiple . in a row cannot be parsed unescaped
      if (glob.contains("..")) {
        backtick(glob, false, true, true)
      } else {
        val escapedGlob = glob.split('.').map(backtick(_, false, true, true)).mkString(".")
        // If we had a trailing . the splitting above would remove it so lets re-add it
        if (glob.isEmpty) "``" else if (glob.last.equals('.')) s"$escapedGlob." else escapedGlob
      }
    }

    def stringify: PartialFunction[PrivilegeQualifier, String] = {
      case LabelQualifier(name)        => backtickEmpty(name)
      case RelationshipQualifier(name) => backtickEmpty(name)
      case ElementQualifier(name)      => backtickEmpty(name)
      case UserQualifier(name)         => escapeName(name)
      case ProcedureQualifier(glob)    => stringifyQualifiedName(glob)
      case FunctionQualifier(glob)     => stringifyQualifiedName(glob)
      case SettingQualifier(glob)      => stringifyQualifiedName(glob)
    }

    def extractPropertyRuleExpression(
      elementTypeQualifiers: Seq[PrivilegeQualifier],
      variable: Option[Variable],
      expression: Expression,
      element: Element
    ) = {
      val elementTypes = Some(elementTypeQualifiers
        .flatMap {
          case lq: LabelQualifier        => Some(backtickEmpty(lq.label))
          case rq: RelationshipQualifier => Some(backtickEmpty(rq.reltype))
          case _                         => None
        }.mkString("|"))
        .filterNot(_.equals(""))
        .map(elementTypes => s":$elementTypes")
        .getOrElse("")

      val variableNameString = variable.map(v => backtickEmpty(v.name))

      def propertyAndWherePrettifier(e: Expression) = {
        val where =
          s"WHERE ${ExpressionStringifier.apply(e => e.asCanonicalStringVal).apply(e)}"
        element match {
          case Node         => s"(${variableNameString.getOrElse("")}$elementTypes) $where"
          case Relationship => s"()-[${variableNameString.getOrElse("")}$elementTypes]-() $where"
        }
      }

      def propertyInElementPrettifier(propertyKeyName: PropertyKeyName, value: Expression) = {

        val where = (varName: String) =>
          s"WHERE $varName.${backtickEmpty(propertyKeyName.name)} = " +
            s"${ExpressionStringifier.apply(value => value.asCanonicalStringVal).apply(value)}"

        element match {
          case Node =>
            val varName = variableNameString.getOrElse("n")
            s"($varName$elementTypes) ${where(varName)}"
          case Relationship =>
            val varName = variableNameString.getOrElse("r")
            s"()-[$varName$elementTypes]-() ${where(varName)}"
        }
      }

      expression match {
        case _ @MapExpression(Seq((propertyKeyName, value))) => propertyInElementPrettifier(propertyKeyName, value)
        case e: Equals                                       => propertyAndWherePrettifier(e)
        case e: NotEquals                                    => propertyAndWherePrettifier(e)
        case e: GreaterThan                                  => propertyAndWherePrettifier(e)
        case e: GreaterThanOrEqual                           => propertyAndWherePrettifier(e)
        case e: LessThan                                     => propertyAndWherePrettifier(e)
        case e: LessThanOrEqual                              => propertyAndWherePrettifier(e)
        case e: IsNull                                       => propertyAndWherePrettifier(e)
        case e: IsNotNull                                    => propertyAndWherePrettifier(e)
        case e @ In(_, _: ListLiteral)                       => propertyAndWherePrettifier(e)
        case e @ In(_, _: ExplicitParameter)                 => propertyAndWherePrettifier(e)
        case e @ In(_, _: Property)                          => propertyAndWherePrettifier(e)
        case e @ Not(innerExpression) => innerExpression match {
            case _: Equals                      => propertyAndWherePrettifier(e)
            case _: NotEquals                   => propertyAndWherePrettifier(e)
            case _: GreaterThan                 => propertyAndWherePrettifier(e)
            case _: GreaterThanOrEqual          => propertyAndWherePrettifier(e)
            case _: LessThan                    => propertyAndWherePrettifier(e)
            case _: LessThanOrEqual             => propertyAndWherePrettifier(e)
            case _: IsNull                      => propertyAndWherePrettifier(e)
            case _: IsNotNull                   => propertyAndWherePrettifier(e)
            case _ @In(_, _: ListLiteral)       => propertyAndWherePrettifier(e)
            case _ @In(_, _: ExplicitParameter) => propertyAndWherePrettifier(e)
            case _ @In(_, _: Property)          => propertyAndWherePrettifier(e)
            case _ => throw new IllegalStateException(
                s"Unknown expression: ${ExpressionStringifier.apply(e => e.asCanonicalStringVal).apply(e)}"
              )
          }
        case e => throw new IllegalStateException(
            s"Unknown expression: ${ExpressionStringifier.apply(e => e.asCanonicalStringVal).apply(e)}"
          )
      }
    }

    qualifier match {
      case l @ LabelQualifier(_) :: Nil           => Some("NODE " + l.map(stringify).mkString(", "))
      case l @ LabelQualifier(_) :: _             => Some("NODES " + l.map(stringify).mkString(", "))
      case LabelAllQualifier() :: Nil             => Some("NODES *")
      case rels @ RelationshipQualifier(_) :: Nil => Some("RELATIONSHIP " + rels.map(stringify).mkString(", "))
      case rels @ RelationshipQualifier(_) :: _   => Some("RELATIONSHIPS " + rels.map(stringify).mkString(", "))
      case RelationshipAllQualifier() :: Nil      => Some("RELATIONSHIPS *")
      case elems @ ElementQualifier(_) :: _       => Some("ELEMENTS " + elems.map(stringify).mkString(", "))
      case ElementsAllQualifier() :: Nil          => Some("ELEMENTS *")
      case PatternQualifier(lqs, v, e, element) :: Nil =>
        Some(s"FOR ${extractPropertyRuleExpression(lqs, v, e, element)}")
      case UserQualifier(user) :: Nil       => Some("(" + escapeName(user) + ")")
      case users @ UserQualifier(_) :: _    => Some("(" + users.map(stringify).mkString(", ") + ")")
      case UserAllQualifier() :: Nil        => Some("(*)")
      case AllQualifier() :: Nil            => None
      case AllDatabasesQualifier() :: Nil   => None
      case p @ ProcedureQualifier(_) :: _   => Some(p.map(stringify).mkString(", "))
      case ProcedureAllQualifier() :: Nil   => Some("*")
      case p @ FunctionQualifier(_) :: _    => Some(p.map(stringify).mkString(", "))
      case FunctionAllQualifier() :: Nil    => Some("*")
      case p @ SettingQualifier(_) :: _     => Some(p.map(stringify).mkString(", "))
      case SettingAllQualifier() :: Nil     => Some("*")
      case SecretAllQualifier() :: Nil      => Some("*")
      case SecretQualifier(Left(s)) :: Nil  => Some(expr.quote(s))
      case SecretQualifier(Right(p)) :: Nil => Some(escapeName(p))
      case _                                => Some("<unknown>")
    }
  }

  private def extractQualifierString(qualifier: List[PrivilegeQualifier], expr: ExpressionStringifier): String = {
    val qualifierPart = extractQualifierPart(qualifier, expr)
    qualifierPart match {
      case Some(string) => s" $string"
      case _            => ""
    }
  }

  private def extractDbScope(dbScope: DatabaseScope): (String, Boolean, Boolean) = dbScope match {
    case SingleNamedDatabaseScope(name)         => (escapeDatabaseName(name), false, false)
    case AllDatabasesScope()                    => ("*", false, false)
    case DefaultDatabaseScope()                 => ("DEFAULT", true, false)
    case HomeDatabaseScope()                    => ("HOME", true, false)
    case NamedDatabasesScope(Seq(databaseName)) => (escapeDatabaseName(databaseName), false, false)
    case NamedDatabasesScope(databaseNames)     => (escapeNames(databaseNames), false, true)
  }

  private def extractGraphScope(graphScope: GraphScope): String = {
    graphScope match {
      case SingleNamedGraphScope(name)  => s"GRAPH ${escapeDatabaseName(name)}"
      case AllGraphsScope()             => "GRAPH *"
      case HomeGraphScope()             => "HOME GRAPH"
      case NamedGraphsScope(Seq(graph)) => s"GRAPH ${escapeDatabaseName(graph)}"
      case NamedGraphsScope(graphs)     => s"GRAPHS ${escapeNames(graphs)}"
    }
  }

  def escapeDatabaseName(name: DatabaseName): String = name match {
    case NamespacedName(names, Some(namespace)) =>
      backtickEmpty(namespace) + "." + backtickEmpty(names.mkString("."))
    case NamespacedName(names, None) => backtickEmpty(names.mkString("."))
    case pn: ParameterName           => "$" + backtickEmpty(pn.parameter.name)
  }

  val escapeName: PartialFunction[Expression, String] = {
    case StringLiteral(s) => backtickEmpty(s)
    case Variable(v)      => backtickEmpty(v)
    case p: Parameter     => s"$$${backtickEmpty(p.name)}"
  }

  private def escapeNames(names: Seq[Expression]): String = names.map(escapeName).mkString(", ")

  private def escapeNames(names: Seq[DatabaseName])(implicit d: DummyImplicit): String =
    names.map(databaseName => escapeDatabaseName(databaseName)).mkString(", ")

  def extractTopology(topology: Topology): String = {
    val primariesString = topology.primaries.flatMap {
      case Left(1)  => Some(s" 1 PRIMARY")
      case Left(n)  => Some(s" $n PRIMARIES")
      case Right(p) => Some(s" $$${backtickEmpty(p.name)} PRIMARIES")
    }.getOrElse("")
    val maybeSecondariesString = topology.secondaries.flatMap {
      case Left(1)  => Some(s" 1 SECONDARY")
      case Left(n)  => Some(s" $n SECONDARIES")
      case Right(p) => Some(s" $$${backtickEmpty(p.name)} SECONDARIES")
    }.getOrElse("")
    s" TOPOLOGY$primariesString$maybeSecondariesString"
  }

  def extractShardTopology(replicas: Option[Either[Int, Parameter]]): String = {
    replicas.flatMap {
      case Left(1)  => Some(" TOPOLOGY 1 REPLICA")
      case Left(n)  => Some(s" TOPOLOGY $n REPLICAS")
      case Right(p) => Some(s" TOPOLOGY $$${backtickEmpty(p.name)} REPLICAS")
    }.getOrElse("")
  }

  private def extractAlterShardDefinition(shardDefinition: ShardDefinition): String = {
    val graphTopology = shardDefinition.graphShardTopology
      .map(Prettifier.extractTopology)
      .map(s => s" SET GRAPH SHARD {SET ${s.trim}}")
      .getOrElse("")
    val shardTopology = shardDefinition.propertyShardReplicaCount.map(s =>
      s" SET PROPERTY SHARD {SET${extractShardTopology(Some(s))}}"
    ).getOrElse("")
    s"$graphTopology$shardTopology"
  }

  private def extractShardDefinition(shardDefinition: ShardDefinition): String = {
    val graphTopology = shardDefinition.graphShardTopology
      .map(Prettifier.extractTopology)
      .map(s => s"GRAPH SHARD {${s.trim}} ")
      .getOrElse("")
    val replicaString = extractShardTopology(shardDefinition.propertyShardReplicaCount)
    s" ${graphTopology}PROPERTY SHARD {COUNT ${shardDefinition.propertyShardCount}$replicaString}"
  }

  private[prettifier] def stringifyOptions(options: Options)(implicit expr: ExpressionStringifier) = options match {
    case NoOptions               => ""
    case OptionsParam(parameter) => s" OPTIONS ${expr(parameter)}"
    case OptionsMap(map)         => optionsToString(map)
  }

  private def optionsToString(options: Map[String, Expression])(implicit expr: ExpressionStringifier): String =
    if (options.nonEmpty) {
      val mapString = options.map { case (s, e) =>
        s"${expr.backtick(s, shouldBacktickEmpty = true)}: ${expr(e)}"
      }.mkString("{", ", ", "}")
      s" OPTIONS $mapString"
    } else {
      " OPTIONS {}"
    }

  def maybeImmutable(immutable: Boolean): String = if (immutable) " IMMUTABLE" else ""

  private def authRuleSetClausesToString(setClauses: List[AuthRuleSetClause])(implicit
    expr: ExpressionStringifier): String =
    setClauses
      .map(clause =>
        (
          clause.name,
          clause match {
            case condition: AuthRuleCondition =>
              expr(condition.expression)
            case enabled: AuthRuleEnabled => enabled.enabled.toString
          }
        )
      ).map { case (name, value) => s"$name $value" }
      .mkString(" ")

  private def userTagsActionAsString(action: UserTagsAction)(implicit expr: ExpressionStringifier): String =
    action match {
      case SetTags(value)    => s"SET TAGS ${expr(value)}"
      case AddTags(value)    => s"ADD TAGS ${expr(value)}"
      case RemoveTags(value) => s"REMOVE TAGS ${expr(value)}"
      case RemoveAllTags()   => "REMOVE ALL TAGS"
    }

}
