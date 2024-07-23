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
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AddedInRewrite
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateFulltextIndex
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateSingleLabelPropertyIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FunctionAllQualifier
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
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
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParameterName
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
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SchemaCommand
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
import org.neo4j.cypher.internal.ast.SettingAllQualifier
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
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
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
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
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.escapeName
import org.neo4j.cypher.internal.expressions.CoerceTo
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

//noinspection DuplicatedCode
case class Prettifier(
  expr: ExpressionStringifier,
  extension: Prettifier.ClausePrettifier = Prettifier.EmptyExtension,
  useInCommands: Boolean = true
) {

  val NL: String = System.lineSeparator()
  val BASE_INDENT: String = "  "

  private val base = IndentingQueryPrettifier()

  def asString(statement: Statement): String = statement match {
    case q: Query                 => base.query(q)
    case c: SchemaCommand         => asString(c)
    case c: AdministrationCommand => asString(c)
    case _                        => throw new IllegalStateException(s"Unknown statement: $statement")
  }

  def asString(hint: Hint): String = base.asString(hint)

  def backtick(s: String): String = expr.backtick(s)

  def propertiesMapToString(name: String, properties: Option[Either[Map[String, Expression], Parameter]]): String =
    properties match {
      case Some(Left(props)) =>
        if (props.nonEmpty) {
          s" $name ${props.map({ case (s, e) => s"${backtick(s)}: ${expr(e)}" }).mkString("{", ", ", "}")}"
        } else {
          s" $name {}"
        }
      case Some(Right(parameter)) => s" $name ${expr(parameter)}"
      case None                   => ""
    }

  def prettifySetItems(setItems: Seq[SetItem]): String = {
    val items = setItems.map {
      case SetPropertyItem(prop, exp)        => s"${expr(prop)} = ${expr(exp)}"
      case SetDynamicPropertyItem(prop, exp) => s"${expr(prop)} = ${expr(exp)}"
      case SetPropertyItems(entity, items) =>
        items.map(i => s"${expr(entity)}.${i._1.name} = ${expr(i._2)}").mkString(", ")
      case SetLabelItem(variable, labels, dynamicLabels, false) => labelsString(variable, labels, dynamicLabels)
      case SetLabelItem(variable, labels, dynamicLabels, true)  => isLabelsString(variable, labels, dynamicLabels)
      case SetIncludingPropertiesFromMapItem(variable, exp)     => s"${expr(variable)} += ${expr(exp)}"
      case SetExactPropertiesFromMapItem(variable, exp)         => s"${expr(variable)} = ${expr(exp)}"
    }
    items.mkString(", ")
  }

  def prettifyRemoveItems(removeItems: Seq[RemoveItem]): String = {
    val items = removeItems.map {
      case RemovePropertyItem(prop)                                => s"${expr(prop)}"
      case RemoveDynamicPropertyItem(dynamicPropertyLookup)        => s"${expr(dynamicPropertyLookup)}"
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
    def propertyToString(property: Property): String = s"${expr(property.map)}.${backtick(property.propertyKey.name)}"

    def getStartOfCommand(
      name: Option[Either[String, Parameter]],
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
        val pattern = entityName match {
          case LabelName(label)     => s"(${backtick(variable)}:${backtick(label)})"
          case RelTypeName(relType) => s"()-[${backtick(variable)}:${backtick(relType)}]-()"
        }
        s"${startOfCommand}FOR $pattern ON ${propertiesToString(properties)}${asString(options)}"

      case CreateLookupIndex(Variable(variable), isNodeIndex, function, name, indexType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val pattern = if (isNodeIndex) s"(${backtick(variable)})" else s"()-[${backtick(variable)}]-()"
        // can't use `expr(functions)` since that might add extra () we can't parse: labels((n))
        val functionString =
          function.name + "(" + function.args.map(e => backtick(e.asCanonicalStringVal)).mkString(", ") + ")"
        s"${startOfCommand}FOR $pattern ON EACH $functionString${asString(options)}"

      case CreateFulltextIndex(Variable(variable), entityNames, properties, name, indexType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, indexType.command)
        val pattern = entityNames match {
          case Left(labels) =>
            val labelPattern = labels.map(l => backtick(l.name)).mkString(":", "|", "")
            s"(${backtick(variable)}$labelPattern)"
          case Right(relTypes) =>
            val relTypePattern = relTypes.map(r => backtick(r.name)).mkString(":", "|", "")
            s"()-[${backtick(variable)}$relTypePattern]-()"
        }
        val propertiesString = properties.map(propertyToString).mkString("[", ", ", "]")
        s"${startOfCommand}FOR $pattern ON EACH $propertiesString${asString(options)}"

      case DropIndexOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP INDEX ${Prettifier.escapeName(name)}$ifExistsString"

      case CreateConstraint(Variable(variable), entityName, properties, name, constraintType, ifExistsDo, options) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        val pattern = entityName match {
          case LabelName(label)     => s"(${backtick(variable)}:${backtick(label)})"
          case RelTypeName(relType) => s"()-[${backtick(variable)}:${backtick(relType)}]-()"
        }
        s"${startOfCommand}FOR $pattern REQUIRE ${propertiesToString(properties)} ${constraintType.predicate}${asString(options)}"

      case DropConstraintOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP CONSTRAINT ${Prettifier.escapeName(name)}$ifExistsString"

      case _ => throw new IllegalStateException(s"Unknown command: $command")
    }
    useString + commandString
  }

  def asString(adminCommand: AdministrationCommand): String = {
    val useString = asString(adminCommand.useGraph)

    def showClausesAsString(yieldOrWhere: YieldOrWhere): (String, String) = {
      val ind: IndentingQueryPrettifier = base.indented()
      yieldOrWhere match {
        case Some(Left((y, r))) => (NL + ind.asString(y), r.map(ind.asString).map(NL + _).getOrElse(""))
        case Some(Right(w))     => (NL + ind.asString(w), "")
        case None               => ("", "")
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

      case x @ CreateUser(userName, userOptions, ifExistsDo, externalAuths, nativeAuth) =>
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
            if (x.useOldStyleNativeAuth)
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
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeName(name)}"
          case _                           => None
        }.getOrElse("")

        val externalAuthString = externalAuths.sortBy(_.provider).map { auth =>
          val ind: IndentingQueryPrettifier = base.indented()
          ind.asString(auth)
        }.mkString

        s"${x.name} $userNameString$ifNotExists$oldStyleNativeAuthString$statusString$homeDatabaseString$setAuthNativeString$externalAuthString"

      case x @ RenameUser(fromUserName, toUserName, ifExists) =>
        Prettifier.prettifyRename(x.name, fromUserName, toUserName, ifExists)

      case x @ DropUser(userName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(userName)} IF EXISTS"
        else s"${x.name} ${Prettifier.escapeName(userName)}"

      case x @ AlterUser(userName, userOptions, ifExists, externalAuths, nativeAuth, removeAuth) =>
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

                if (x.useOldStyleNativeAuth) (s" $passwordClauseString${changeString.getOrElse("")}", "")
                else {
                  val ind: IndentingQueryPrettifier = base.indented()
                  val authString =
                    if (changeString.nonEmpty)
                      ind.getNativeAuthAsString(passwordClauseString, s"SET PASSWORD${changeString.get}")
                    else ind.getNativeAuthAsString(passwordClauseString)
                  ("", authString)
                }

              case (None, Some(changeString)) =>
                if (x.useOldStyleNativeAuth) (s" SET PASSWORD$changeString", "")
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
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeName(name)}"
        }.getOrElse("")

        val externalAuthString = externalAuths.sortBy(_.provider).map { auth =>
          val ind: IndentingQueryPrettifier = base.indented()
          ind.asString(auth)
        }.mkString

        s"${x.name} $userNameString$ifExistsString$removeHomeDatabase$removeAuthString$oldStyleNativeAuthString$statusString$setHomeDatabaseString$setAuthNativeString$externalAuthString"

      case x @ SetOwnPassword(newPassword, currentPassword) =>
        s"${x.name} FROM ${expr.escapePassword(currentPassword)} TO ${expr.escapePassword(newPassword)}"

      // Role commands

      case x @ ShowRoles(withUsers, _, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}${if (withUsers) " WITH USERS" else ""}$y$r"

      case x @ CreateRole(roleName, None, ifExistsDo) =>
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax =>
            s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS"
          case _ => s"${x.name} ${Prettifier.escapeName(roleName)}"
        }

      case x @ CreateRole(roleName, Some(fromRole), ifExistsDo) =>
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax =>
            s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS AS COPY OF ${Prettifier.escapeName(fromRole)}"
          case _ => s"${x.name} ${Prettifier.escapeName(roleName)} AS COPY OF ${Prettifier.escapeName(fromRole)}"
        }

      case x @ RenameRole(fromRoleName, toRoleName, ifExists) =>
        Prettifier.prettifyRename(x.name, fromRoleName, toRoleName, ifExists)

      case x @ DropRole(roleName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(roleName)} IF EXISTS"
        else s"${x.name} ${Prettifier.escapeName(roleName)}"

      case x @ GrantRolesToUsers(roleNames, userNames) =>
        val start = if (roleNames.length > 1) s"${x.name}S" else x.name
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ RevokeRolesFromUsers(roleNames, userNames) =>
        val start = if (roleNames.length > 1) s"${x.name}S" else x.name
        s"$start ${roleNames.map(Prettifier.escapeName).mkString(", ")} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      // Privilege commands
      // dbms privileges

      case x @ GrantPrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(DbmsPrivilege(_), _, _, qualifiers, roleNames, _) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

      // database privileges

      case x @ GrantPrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ DenyPrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ RevokePrivilege(DatabasePrivilege(_, dbScope), _, _, qualifier, roleNames, _) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "FROM", roleNames)

      // graph privileges

      case x @ GrantPrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ DenyPrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ RevokePrivilege(GraphPrivilege(action, graphScope), _, resource, qualifier, roleNames, _) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
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

      case x @ ShowDatabase(scope, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val optionalName = scope match {
          case SingleNamedDatabaseScope(dbName) => s" ${Prettifier.escapeName(dbName)}"
          case _                                => ""
        }
        s"${x.name}$optionalName$y$r"

      case x @ CreateDatabase(dbName, ifExistsDo, options, waitUntilComplete, topology) =>
        val formattedOptions = asString(options)
        val withoutNamespace = dbName match {
          case n: NamespacedName => Left(n.toString)
          case ParameterName(p)  => Right(p)
        }
        val maybeTopologyString = topology.map(Prettifier.extractTopology).getOrElse("")
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax =>
            s"${x.name} ${Prettifier.escapeName(withoutNamespace)} IF NOT EXISTS$maybeTopologyString$formattedOptions${waitUntilComplete.name}"
          case _ =>
            s"${x.name} ${Prettifier.escapeName(withoutNamespace)}$maybeTopologyString$formattedOptions${waitUntilComplete.name}"
        }

      case x @ CreateCompositeDatabase(name, ifExistsDo, options, waitUntilComplete) =>
        val formattedOptions = asString(options)
        val ifExists = ifExistsDo match {
          case IfExistsInvalidSyntax | IfExistsDoNothing => " IF NOT EXISTS"
          case _                                         => ""
        }
        s"${x.name} ${escapeName(name)}$ifExists$formattedOptions${waitUntilComplete.name}"

      case x @ DropDatabase(dbName, ifExists, _, additionalAction, waitUntilComplete) =>
        (ifExists, additionalAction) match {
          case (false, DestroyData) =>
            s"${x.name} ${Prettifier.escapeName(dbName)} DESTROY DATA${waitUntilComplete.name}"
          case (true, DestroyData) =>
            s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DESTROY DATA${waitUntilComplete.name}"
          case (false, DumpData) =>
            s"${x.name} ${Prettifier.escapeName(dbName)} DUMP DATA${waitUntilComplete.name}"
          case (true, DumpData) =>
            s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DUMP DATA${waitUntilComplete.name}"
        }

      case x @ AlterDatabase(dbName, ifExists, access, topology, options, optionsToRemove, waitUntilComplete) =>
        val maybeAccessString = access.map(getAccessString).getOrElse("")
        val maybeIfExists = if (ifExists) " IF EXISTS" else ""
        val maybeTopologyString = topology.map(topo => s" SET${Prettifier.extractTopology(topo)}").getOrElse("")
        val formattedOptions = asIndividualOptions(options)
        val formattedOptionsToRemove = optionsToRemove.map(o => s" REMOVE OPTION ${backtick(o)}").mkString("")
        s"${x.name} ${Prettifier.escapeName(dbName)}$maybeIfExists$maybeAccessString$maybeTopologyString$formattedOptions$formattedOptionsToRemove${waitUntilComplete.name}"

      case x @ StartDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}${waitUntilComplete.name}"

      case x @ StopDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}${waitUntilComplete.name}"

      case x @ CreateLocalDatabaseAlias(aliasName, targetName, ifExistsDo, properties) =>
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax =>
            s"${x.name} ${Prettifier.escapeName(aliasName)} IF NOT EXISTS FOR DATABASE ${Prettifier.escapeName(targetName)}$propertiesString"
          case _ =>
            s"${x.name} ${Prettifier.escapeName(aliasName)} FOR DATABASE ${Prettifier.escapeName(targetName)}$propertiesString"
        }

      case x @ CreateRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExistsDo,
          url,
          username,
          password,
          driverSettings,
          properties
        ) =>
        val urlString = url match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }

        val driverSettingsString = propertiesMapToString("DRIVER", driverSettings)
        val propertiesString = propertiesMapToString("PROPERTIES", properties)

        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax =>
            s"${x.name} ${Prettifier.escapeName(aliasName)} IF NOT EXISTS FOR DATABASE ${Prettifier.escapeName(targetName)} AT $urlString " +
              s"USER ${Prettifier.escapeName(username)} PASSWORD ${expr.escapePassword(password)}" +
              driverSettingsString + propertiesString
          case _ =>
            s"${x.name} ${Prettifier.escapeName(aliasName)} FOR DATABASE ${Prettifier.escapeName(targetName)} AT $urlString " +
              s"USER ${Prettifier.escapeName(username)} PASSWORD ${expr.escapePassword(password)}" +
              driverSettingsString + propertiesString
        }

      case x @ DropDatabaseAlias(aliasName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(aliasName)} IF EXISTS FOR DATABASE"
        else s"${x.name} ${Prettifier.escapeName(aliasName)} FOR DATABASE"

      case x @ AlterLocalDatabaseAlias(aliasName, targetName, ifExists, properties) =>
        val target = targetName.map(tgt => "TARGET " + Prettifier.escapeName(tgt)).getOrElse("")
        val propertiesString = propertiesMapToString("PROPERTIES", properties)
        if (ifExists)
          s"${x.name} ${Prettifier.escapeName(aliasName)} IF EXISTS SET DATABASE $target$propertiesString"
        else s"${x.name} ${Prettifier.escapeName(aliasName)} SET DATABASE $target$propertiesString"

      case x @ AlterRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExists,
          url,
          username,
          password,
          driverSettings,
          properties
        ) =>
        val targetString = targetName match {
          case Some(targetName) =>
            val urlString = url match {
              case Some(Left(s))          => s" AT ${expr.quote(s)}"
              case Some(Right(parameter)) => s" AT ${expr(parameter)}"
              case _                      => ""
            }
            s" TARGET ${Prettifier.escapeName(targetName)}$urlString"
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

        if (ifExists)
          s"${x.name} ${Prettifier.escapeName(aliasName)} IF EXISTS SET DATABASE$targetString$userString$passwordString$driverSettingsString$propertiesString"
        else
          s"${x.name} ${Prettifier.escapeName(aliasName)} SET DATABASE$targetString$userString$passwordString$driverSettingsString$propertiesString"

      case x @ ShowAliases(aliasName, yields, _) =>
        val an = aliasName.map(an => s" ${escapeName(an)}").getOrElse("")
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$an FOR DATABASE$y$r"

      case x @ EnableServer(serverName, options) =>
        val name = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        val optionString = asString(options)
        s"${x.name} $name$optionString"

      case x @ AlterServer(serverName, options) =>
        val name = serverName match {
          case Left(s)          => expr.quote(s)
          case Right(parameter) => expr(parameter)
        }
        val optionString = asString(options)
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
        if (dryRun) s"DRYRUN ${x.name}"
        else x.name

      case command => throw new InternalError(s"Unexpected command $command")
    }
    useString + commandString
  }

  private def asString(use: Option[GraphSelection]) = {
    use.filter(_ => useInCommands).map(u => base.dispatch(u) + NL).getOrElse("")
  }

  private def asString(options: Options) = options match {
    case NoOptions               => ""
    case OptionsParam(parameter) => s" OPTIONS ${expr(parameter)}"
    case OptionsMap(map)         => optionsToString(map)
  }

  private def optionsToString(options: Map[String, Expression]): String =
    if (options.nonEmpty)
      s" OPTIONS ${options.map({ case (s, e) => s"${backtick(s)}: ${expr(e)}" }).mkString("{", ", ", "}")}"
    else {
      " OPTIONS {}"
    }

  private def asIndividualOptions(options: Options) = options match {
    case NoOptions => ""
    case OptionsMap(map) => map.map {
        case (key, value) => s" SET OPTION ${backtick(key)} ${expr(value)}"
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
      }

    def asString(clause: Clause): String = dispatch(clause)

    def dispatch(clause: Clause): String = clause match {
      case u: UseGraph                    => asString(u)
      case e: Return                      => asString(e)
      case f: Finish                      => asString(f)
      case m: Match                       => asString(m)
      case c: SubqueryCall                => asString(c)
      case w: With                        => asString(w)
      case y: Yield                       => asString(y)
      case c: Create                      => asString(c)
      case i: Insert                      => asString(i)
      case u: Unwind                      => asString(u)
      case u: UnresolvedCall              => asString(u)
      case s: ShowIndexesClause           => asString(s)
      case s: ShowConstraintsClause       => asString(s)
      case s: ShowProceduresClause        => asString(s)
      case s: ShowFunctionsClause         => asString(s)
      case s: ShowTransactionsClause      => asString(s)
      case t: TerminateTransactionsClause => asString(t)
      case s: ShowSettingsClause          => asString(s)
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
        case GraphFunctionReference(functionInvocation: FunctionInvocation) =>
          s"${INDENT}USE ${expr(functionInvocation)}"
      }
    }

    def asString(m: Match): String = {
      val o = if (m.optional) "OPTIONAL " else ""
      val mm = appendSpaceIfNonEmpty(m.matchMode.prettified)
      val p = expr.patterns.apply(m.pattern)
      val ind = indented()
      val w = m.where.map(ind.asString).map(asNewLine).getOrElse("")
      val h = m.hints.map(ind.asString).map(asNewLine).mkString
      s"$INDENT${o}MATCH $mm$p$h$w"
    }

    def asString(c: SubqueryCall): String = {
      val inTxParams = c.inTransactionsParameters.map(asString).getOrElse("")
      s"""${INDENT}CALL {
         |${indented().query(c.innerQuery)}
         |$INDENT}$inTxParams""".stripMargin
    }

    def asString(ip: InTransactionsParameters): String = {
      val ofRows = ip.batchParams.map(_.batchSize) match {
        case Some(size) => " OF " + expr(size) + " ROWS"
        case None       => ""
      }
      val concurrency = ip.concurrencyParams match {
        case Some(InTransactionsConcurrencyParameters(Some(explicit))) => " " + expr(explicit) + " CONCURRENT"
        case Some(InTransactionsConcurrencyParameters(None))           => " CONCURRENT"
        case None                                                      => ""
      }
      val onError = ip.errorParams.map(_.behaviour) match {
        case Some(OnErrorBreak)    => s" ON ERROR BREAK"
        case Some(OnErrorContinue) => s" ON ERROR CONTINUE"
        case Some(OnErrorFail)     => s" ON ERROR FAIL"
        case None                  => ""
      }
      val reportStatus = ip.reportParams.map(_.reportAs) match {
        case Some(statusVar) => s" REPORT STATUS AS ${ExpressionStringifier.backtick(statusVar.name)}"
        case None            => ""
      }
      s" IN$concurrency TRANSACTIONS$ofRows$onError$reportStatus"
    }

    def asString(w: Where): String =
      s"${INDENT}WHERE ${expr(w.expression)}"

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
            expr(v),
            ":",
            expr(l),
            ps.map(expr(_)).mkString("(", ",", ")")
          ).mkString

        case UsingScanHint(v, l) => Seq(
            s"${INDENT}USING SCAN ",
            expr(v),
            ":",
            expr(l)
          ).mkString

        case UsingJoinHint(vs) => Seq(
            s"${INDENT}USING JOIN ON ",
            vs.map(expr(_)).toIterable.mkString(", ")
          ).mkString

        // Note: This hint cannot be written in Cypher.
        case UsingStatefulShortestPathAll(vs) => Seq(
            s"${INDENT}USING SSP_ALL ON ",
            vs.map(expr(_)).toIterable.mkString(", ")
          ).mkString

        // Note: This hint cannot be written in Cypher.
        case UsingStatefulShortestPathInto(vs) => Seq(
            s"${INDENT}USING SSP_INTO ON ",
            vs.map(expr(_)).toIterable.mkString(", ")
          ).mkString
      }
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

    def asString(o: Skip): String = s"${INDENT}SKIP ${expr(o.expression)}"
    def asString(o: Limit): String = s"${INDENT}LIMIT ${expr(o.expression)}"

    def asString(o: OrderBy): String = s"${INDENT}ORDER BY " + {
      o.sortItems.map {
        case AscSortItem(expression)  => expr(expression) + " ASCENDING"
        case DescSortItem(expression) => expr(expression) + " DESCENDING"
      }.mkString(", ")
    }

    def asString(r: ReturnItem): String = r match {
      case AliasedReturnItem(e, v)   => expr(e) + " AS " + expr(v)
      case UnaliasedReturnItem(e, _) => expr(e)
    }

    def asString(r: ReturnItems): String = {
      val as = if (r.includeExisting) Seq("*") else Seq()
      val is = r.items.map(asString)
      (as ++ is).mkString(", ")
    }

    def asString(r: Return): String =
      if (r.addedInRewrite) ""
      else {
        val d = if (r.distinct) " DISTINCT" else ""
        val i = asString(r.returnItems)
        val ind = indented()
        val o = r.orderBy.map(ind.asString).map(asNewLine).getOrElse("")
        val l = r.limit.map(ind.asString).map(asNewLine).getOrElse("")
        val s = r.skip.map(ind.asString).map(asNewLine).getOrElse("")
        s"${INDENT}RETURN$d $i$o$s$l"
      }

    def asString(f: Finish): String = s"${INDENT}FINISH"

    def asString(w: With): String = {
      val ind = indented()
      val rewrittenClauses = List(
        w.orderBy.map(ind.asString),
        w.skip.map(ind.asString),
        w.limit.map(ind.asString),
        w.where.map(ind.asString)
      ).flatten

      if (w.withType == ParsedAsYield || w.withType == AddedInRewrite) {
        // part of SHOW/TERMINATE TRANSACTION which prettifies the YIELD items part
        // but it no longer knows the subclauses, hence prettifying them here

        // only add newlines between subclauses and not in front of the first one
        if (rewrittenClauses.nonEmpty)
          s"$INDENT${rewrittenClauses.head}${rewrittenClauses.tail.map(asNewLine).mkString}"
        else ""
      } else {
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
      s"${INDENT}UNWIND ${expr(u.expression)} AS ${expr(u.variable)}"
    }

    def asString(u: UnresolvedCall): String = {
      val namespace = expr(u.procedureNamespace)
      val prefix = if (namespace.isEmpty) "" else namespace + "."
      val args = u.declaredArguments.map(_.filter {
        case CoerceTo(_: ImplicitProcedureArgument, _) => false
        case _: ImplicitProcedureArgument              => false
        case _                                         => true
      })
      val arguments = args.map(list => list.map(expr(_)).mkString("(", ", ", ")")).getOrElse("")
      val ind = indented()
      val yields =
        if (u.yieldAll) asNewLine(s"${indented().INDENT}YIELD *")
        else u.declaredResult.filter(_.items.nonEmpty).map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}CALL $prefix${expr(u.procedureName)}$arguments$yields"
    }

    def asString(r: ProcedureResult): String = {
      def item(i: ProcedureResultItem) = i.output.map(expr(_) + " AS ").getOrElse("") + expr(i.variable)
      val items = r.items.map(item).mkString(", ")
      val ind = indented()
      val where = r.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}YIELD $items$where"
    }

    def asString(s: ShowIndexesClause): String = {
      val indexType = s.indexType.prettyPrint
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"SHOW $indexType INDEXES$where$yielded"
    }

    def asString(s: ShowConstraintsClause): String = {
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"SHOW ${s.constraintType.prettyPrint} CONSTRAINTS$where$yielded"
    }

    def asString(s: ShowProceduresClause): String = {
      val executable = getExecutablePart(s.executable)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"${s.name}$executable$where$yielded"
    }

    def asString(s: ShowFunctionsClause): String = {
      val functionType = s.functionType.prettyPrint
      val executable = getExecutablePart(s.executable)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"SHOW $functionType FUNCTIONS$executable$where$yielded"
    }

    private def getExecutablePart(executable: Option[ExecutableBy]): String = executable match {
      case Some(CurrentUser) => " EXECUTABLE BY CURRENT USER"
      case Some(User(name))  => s" EXECUTABLE BY ${ExpressionStringifier.backtick(name)}"
      case None              => ""
    }

    def asString(s: ShowTransactionsClause): String = {
      val ids = namesAsString(s.names)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"SHOW TRANSACTIONS$ids$where$yielded"
    }

    def asString(s: TerminateTransactionsClause): String = {
      val ids = namesAsString(s.names)
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"TERMINATE TRANSACTIONS$ids$yielded"
    }

    def asString(s: ShowSettingsClause): String = {
      val names = namesAsString(s.names)
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      val yielded = partialYieldAsString(s.yieldItems, s.yieldAll)
      s"${s.name}$names$where$yielded"
    }

    private def namesAsString(ids: Either[List[String], Expression]): String = ids match {
      case Left(s)  => if (s.nonEmpty) s.map(id => expr.quote(id)).mkString(" ", ", ", "") else ""
      case Right(e) => s" ${expr(e)}"
    }

    private def partialYieldAsString(yieldItems: List[CommandResultItem], yieldAll: Boolean): String =
      if (yieldItems.nonEmpty) {
        val items = yieldItems.map(c => {
          if (!c.aliasedVariable.name.equals(c.originalName)) {
            backtick(c.originalName) + " AS " + expr(c.aliasedVariable)
          } else expr(c.aliasedVariable)
        }).mkString(", ")
        asNewLine(s"${INDENT}YIELD $items")
      } else if (yieldAll) asNewLine(s"${INDENT}YIELD *")
      else ""

    def asString(s: SetClause): String = {
      s"${INDENT}SET ${prettifySetItems(s.items)}"
    }

    def asString(r: Remove): String = {
      s"${INDENT}REMOVE ${prettifyRemoveItems(r.items)}"
    }

    def asString(v: LoadCSV): String = {
      val withHeaders = if (v.withHeaders) " WITH HEADERS" else ""
      val url = expr(v.urlString)
      val varName = expr(v.variable)
      val fieldTerminator = v.fieldTerminator.map(x => " FIELDTERMINATOR " + expr(x)).getOrElse("")
      s"${INDENT}LOAD CSV$withHeaders FROM $url AS $varName$fieldTerminator"
    }

    def asString(delete: Delete): String = {
      val detach = if (delete.forced) "DETACH " else ""
      s"$INDENT${detach}DELETE ${delete.expressions.map(expr(_)).mkString(", ")}"
    }

    def asString(foreach: Foreach): String = {
      val varName = expr(foreach.variable)
      val list = expr(foreach.expression)
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

  trait QueryPrettifier {
    def INDENT: String
    def asString(clause: Clause): String
  }

  trait ClausePrettifier {
    def asString(ctx: QueryPrettifier): PartialFunction[Clause, String]
  }

  object EmptyExtension extends ClausePrettifier {
    def asString(ctx: QueryPrettifier): PartialFunction[Clause, String] = PartialFunction.empty
  }

  def prettifyRename(
    commandName: String,
    fromName: Expression,
    toName: Expression,
    ifExists: Boolean
  ): String = {
    val maybeIfExists = if (ifExists) " IF EXISTS" else ""
    s"$commandName ${escapeName(fromName)}$maybeIfExists TO ${escapeName(toName)}"
  }

  def extractScope(scope: ShowPrivilegeScope): String = {
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
      case _                   => "<unknown>"
    }
  }

  def revokeOperation(operation: String, revokeType: String) = s"$operation($revokeType)"

  def prettifyDatabasePrivilege(
    privilegeName: String,
    dbScope: DatabaseScope,
    qualifier: List[PrivilegeQualifier],
    preposition: String,
    roleNames: Seq[Expression]
  ): String = {
    val (dbName, default, multiple) = Prettifier.extractDbScope(dbScope)
    val db =
      if (default) {
        s"$dbName DATABASE"
      } else if (multiple) {
        s"DATABASES $dbName"
      } else {
        s"DATABASE $dbName"
      }
    s"$privilegeName${extractQualifierString(qualifier)} ON $db $preposition ${escapeNames(roleNames)}"
  }

  def prettifyGraphPrivilege(
    privilegeName: String,
    graphScope: GraphScope,
    qualifierString: String,
    resource: Option[ActionResource],
    preposition: String,
    roleNames: Seq[Expression]
  ): String = {

    val resourceName = resource match {
      case Some(PropertyResource(name))    => s" {${ExpressionStringifier.backtick(name)}}"
      case Some(PropertiesResource(names)) => s" {${names.map(ExpressionStringifier.backtick(_)).mkString(", ")}}"
      case Some(AllPropertyResource())     => " {*}"
      case Some(LabelResource(name))       => s" ${ExpressionStringifier.backtick(name)}"
      case Some(LabelsResource(names))     => s" ${names.map(ExpressionStringifier.backtick(_)).mkString(", ")}"
      case Some(AllLabelResource())        => " *"
      case None                            => ""
      case _                               => throw new IllegalStateException(s"Unknown resource: $resource")
    }
    val scope = s"${extractGraphScope(graphScope)}"
    s"$privilegeName$resourceName ON $scope$qualifierString $preposition ${Prettifier.escapeNames(roleNames)}"
  }

  def prettifyLoadPrivilegeQualifier(
    expr: ExpressionStringifier
  ): PartialFunction[List[PrivilegeQualifier], String] = {
    case LoadAllQualifier() :: Nil                  => s"ALL DATA"
    case LoadUrlQualifier(Left(urlString)) :: Nil   => s"URL ${expr.quote(urlString)}"
    case LoadUrlQualifier(Right(urlParam)) :: Nil   => s"URL ${expr(urlParam)}"
    case LoadCidrQualifier(Left(cidrString)) :: Nil => s"CIDR ${expr.quote(cidrString)}"
    case LoadCidrQualifier(Right(cidrParam)) :: Nil => s"CIDR ${expr(cidrParam)}"
  }

  def prettifyGraphQualifier(action: GraphAction, qualifier: List[PrivilegeQualifier]): String = {
    // For WRITE, we don't want to print out the qualifier. For SET and REMOVE LABEL, it is printed out in another position.
    if (action.name.equals("WRITE") || action.name.equals("SET LABEL") || action.name.equals("REMOVE LABEL")) {
      ""
    } else {
      extractQualifierString(qualifier)
    }
  }

  private def extractQualifierPart(qualifier: List[PrivilegeQualifier]): Option[String] = {
    def stringifyQualifiedName(glob: String) = {
      // If we have multiple . in a row, just escape the whole thing to not loose any of them
      // or risk breaking parsing of the prettified string, as multiple . in a row cannot be parsed unescaped
      if (glob.contains("..")) {
        ExpressionStringifier.backtick(glob, globbing = true)
      } else {
        val escapedGlob = glob.split('.').map(ExpressionStringifier.backtick(_, globbing = true)).mkString(".")
        // If we had a trailing . the splitting above would remove it so lets re-add it
        if (glob.last.equals('.')) s"$escapedGlob." else escapedGlob
      }
    }

    def stringify: PartialFunction[PrivilegeQualifier, String] = {
      case LabelQualifier(name)        => ExpressionStringifier.backtick(name)
      case RelationshipQualifier(name) => ExpressionStringifier.backtick(name)
      case ElementQualifier(name)      => ExpressionStringifier.backtick(name)
      case UserQualifier(name)         => escapeName(name)
      case ProcedureQualifier(glob)    => stringifyQualifiedName(glob)
      case FunctionQualifier(glob)     => stringifyQualifiedName(glob)
      case SettingQualifier(glob)      => stringifyQualifiedName(glob)
    }

    def extractPropertyRuleExpression(
      labelQualifiers: Seq[PrivilegeQualifier],
      variable: Option[Variable],
      expression: Expression
    ) = {
      val labels = Some(labelQualifiers
        .flatMap {
          case lq: LabelQualifier => Some(ExpressionStringifier.backtick(lq.label))
          case _                  => None
        }.mkString("|"))
        .filterNot(_.equals(""))
        .map(labels => s":$labels")
        .getOrElse("")

      val variableNameString = variable.map(v => ExpressionStringifier.backtick(v.name))

      def propertyAndWherePrettifier(e: Expression) =
        s"(${variableNameString.getOrElse("")}$labels) WHERE ${ExpressionStringifier.apply(e => e.asCanonicalStringVal).apply(e)}"

      def propertyInNodePrettifier(propertyKeyName: PropertyKeyName, value: Expression) =
        s"(${variableNameString.getOrElse("n")}$labels) " +
          s"WHERE ${variableNameString.getOrElse("n")}.${ExpressionStringifier.backtick(propertyKeyName.name)} = " +
          s"${ExpressionStringifier.apply(value => value.asCanonicalStringVal).apply(value)}"

      expression match {
        case _ @MapExpression(Seq((propertyKeyName, value))) => propertyInNodePrettifier(propertyKeyName, value)
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
      case PatternQualifier(lqs, v, e) :: Nil =>
        Some(s"FOR ${extractPropertyRuleExpression(lqs, v, e)}")
      case UserQualifier(user) :: Nil     => Some("(" + escapeName(user) + ")")
      case users @ UserQualifier(_) :: _  => Some("(" + users.map(stringify).mkString(", ") + ")")
      case UserAllQualifier() :: Nil      => Some("(*)")
      case AllQualifier() :: Nil          => None
      case AllDatabasesQualifier() :: Nil => None
      case p @ ProcedureQualifier(_) :: _ => Some(p.map(stringify).mkString(", "))
      case ProcedureAllQualifier() :: Nil => Some("*")
      case p @ FunctionQualifier(_) :: _  => Some(p.map(stringify).mkString(", "))
      case FunctionAllQualifier() :: Nil  => Some("*")
      case p @ SettingQualifier(_) :: _   => Some(p.map(stringify).mkString(", "))
      case SettingAllQualifier() :: Nil   => Some("*")
      case _                              => Some("<unknown>")
    }
  }

  private def extractQualifierString(qualifier: List[PrivilegeQualifier]): String = {
    val qualifierPart = extractQualifierPart(qualifier)
    qualifierPart match {
      case Some(string) => s" $string"
      case _            => ""
    }
  }

  def extractDbScope(dbScope: DatabaseScope): (String, Boolean, Boolean) = dbScope match {
    case SingleNamedDatabaseScope(name)         => (escapeName(name), false, false)
    case AllDatabasesScope()                    => ("*", false, false)
    case DefaultDatabaseScope()                 => ("DEFAULT", true, false)
    case HomeDatabaseScope()                    => ("HOME", true, false)
    case NamedDatabasesScope(Seq(databaseName)) => (escapeName(databaseName), false, false)
    case NamedDatabasesScope(databaseNames)     => (escapeNames(databaseNames), false, true)
  }

  def extractGraphScope(graphScope: GraphScope): String = {
    graphScope match {
      case SingleNamedGraphScope(name)  => s"GRAPH ${escapeName(name)}"
      case AllGraphsScope()             => "GRAPH *"
      case DefaultGraphScope()          => "DEFAULT GRAPH"
      case HomeGraphScope()             => "HOME GRAPH"
      case NamedGraphsScope(Seq(graph)) => s"GRAPH ${escapeName(graph)}"
      case NamedGraphsScope(graphs)     => s"GRAPHS ${escapeNames(graphs)}"
    }
  }

  def escapeName(name: Either[String, Parameter]): String = name match {
    case Left(s)  => ExpressionStringifier.backtick(s)
    case Right(p) => s"$$${ExpressionStringifier.backtick(p.name)}"
  }

  def escapeName(name: DatabaseName)(implicit d: DummyImplicit): String = name match {
    case NamespacedName(names, Some(namespace)) =>
      ExpressionStringifier.backtick(namespace) + "." + ExpressionStringifier.backtick(names.mkString("."))
    case NamespacedName(names, None) => ExpressionStringifier.backtick(names.mkString("."))
    case ParameterName(p)            => "$" + ExpressionStringifier.backtick(p.name)
  }

  val escapeName: PartialFunction[Expression, String] = {
    case StringLiteral(s) => ExpressionStringifier.backtick(s)
    case p: Parameter     => s"$$${ExpressionStringifier.backtick(p.name)}"
  }

  def escapeNames(names: Seq[Expression]): String = names.map(escapeName).mkString(", ")

  def escapeNames(names: Seq[DatabaseName])(implicit d: DummyImplicit): String =
    names.map(databaseName => escapeName(databaseName)).mkString(", ")

  def extractTopology(topology: Topology): String = {
    val primariesString = topology.primaries.flatMap {
      case 1 => Some(s" 1 PRIMARY")
      case n => Some(s" $n PRIMARIES")
    }.getOrElse("")
    val maybeSecondariesString = topology.secondaries.flatMap {
      case 1 => Some(s" 1 SECONDARY")
      case n => Some(s" $n SECONDARIES")
    }.getOrElse("")
    s" TOPOLOGY$primariesString$maybeSecondariesString"
  }

}
