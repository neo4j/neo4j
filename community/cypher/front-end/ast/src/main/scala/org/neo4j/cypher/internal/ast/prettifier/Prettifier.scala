/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllNodes
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AllRelationships
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FunctionAllQualifier
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NodeByIds
import org.neo4j.cypher.internal.ast.NodeByParameter
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OrderBy
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
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipByIds
import org.neo4j.cypher.internal.ast.RelationshipByParameter
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowConstraints
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Start
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParameterWithOldSyntax
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable

//noinspection DuplicatedCode
case class Prettifier(
  expr: ExpressionStringifier,
  extension: Prettifier.ClausePrettifier = Prettifier.EmptyExtension,
  useInCommands: Boolean = true
) {

  private val NL = System.lineSeparator()

  private val base = IndentingQueryPrettifier()

  def asString(statement: Statement): String = statement match {
    case q: Query                 => base.query(q)
    case c: SchemaCommand         => asString(c)
    case c: AdministrationCommand => asString(c)
    case _ => throw new IllegalStateException(s"Unknown statement: $statement")
  }

  def asString(command: SchemaCommand): String = {
    def backtick(s: String) = ExpressionStringifier.backtick(s)
    def propertiesToString(properties: Seq[Property]): String = properties.map(propertyToString).mkString("(", ", ", ")")
    def propertyToString(property: Property): String = s"${expr(property.map)}.${ExpressionStringifier.backtick(property.propertyKey.name)}"
    def propertyToStringExistenceConstraint(property: Property, oldSyntax: Boolean): String =
      if (oldSyntax) s"exists(${propertyToString(property)})" else s"(${propertyToString(property)}) IS NOT NULL"
    def getStartOfCommand(name: Option[String], ifExistsDo: IfExistsDo, schemaType: String): String = {
      val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
      ifExistsDo match {
        case IfExistsDoNothing     => s"CREATE $schemaType ${nameString}IF NOT EXISTS "
        case IfExistsInvalidSyntax => s"CREATE OR REPLACE $schemaType ${nameString}IF NOT EXISTS "
        case IfExistsReplace       => s"CREATE OR REPLACE $schemaType $nameString"
        case IfExistsThrowError    => s"CREATE $schemaType $nameString"
      }
    }
    def optionsToString(options: Map[String, Expression]) =
      if (options.nonEmpty) s" OPTIONS ${options.map({ case (s, e) => s"${backtick(s)}: ${expr(e)}" }).mkString("{", ", ", "}")}" else ""

    val useString = asString(command.useGraph)
    val commandString = command match {

      case CreateIndexOldSyntax(LabelName(label), properties, _) =>
        s"CREATE INDEX ON :${backtick(label)}${properties.map(p => backtick(p.name)).mkString("(", ", ", ")")}"

      case CreateIndex(Variable(variable), LabelName(label), properties, name, ifExistsDo, options, _) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "INDEX")
        s"${startOfCommand}FOR (${backtick(variable)}:${backtick(label)}) ON ${propertiesToString(properties)}${optionsToString(options)}"

      case DropIndex(LabelName(label), properties, _) =>
        s"DROP INDEX ON :${backtick(label)}${properties.map(p => backtick(p.name)).mkString("(", ", ", ")")}"

      case DropIndexOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP INDEX ${backtick(name)}$ifExistsString"

      case CreateNodeKeyConstraint(Variable(variable), LabelName(label), properties, name, ifExistsDo, options, _) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        s"${startOfCommand}ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS NODE KEY${optionsToString(options)}"

      case DropNodeKeyConstraint(Variable(variable), LabelName(label), properties, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS NODE KEY"

      case CreateUniquePropertyConstraint(Variable(variable), LabelName(label), properties, name, ifExistsDo, options, _) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        s"${startOfCommand}ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS UNIQUE${optionsToString(options)}"

      case DropUniquePropertyConstraint(Variable(variable), LabelName(label), properties, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS UNIQUE"

      case CreateNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, name, ifExistsDo, oldSyntax, options, _) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        val propertyString = propertyToStringExistenceConstraint(property, oldSyntax)
        val optionsString = options.map(o => if (o.nonEmpty) optionsToString(o) else " OPTIONS {}").getOrElse("")
        s"${startOfCommand}ON (${backtick(variable)}:${backtick(label)}) ASSERT $propertyString$optionsString"

      case DropNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT exists(${propertyToString(property)})"

      case CreateRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, name, ifExistsDo, oldSyntax, options, _) =>
        val startOfCommand = getStartOfCommand(name, ifExistsDo, "CONSTRAINT")
        val propertyString = propertyToStringExistenceConstraint(property, oldSyntax)
        val optionsString = options.map(o => if (o.nonEmpty) optionsToString(o) else " OPTIONS {}").getOrElse("")
        s"${startOfCommand}ON ()-[${backtick(variable)}:${backtick(relType)}]-() ASSERT $propertyString$optionsString"

      case DropRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, _) =>
        s"DROP CONSTRAINT ON ()-[${backtick(variable)}:${backtick(relType)}]-() ASSERT exists(${propertyToString(property)})"

      case DropConstraintOnName(name, ifExists, _) =>
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        s"DROP CONSTRAINT ${backtick(name)}$ifExistsString"

      case ShowConstraints(constraintType, verbose, _) =>
        val output = if (verbose) "VERBOSE" else "BRIEF"
        s"SHOW ${constraintType.prettyPrint} CONSTRAINTS $output"

      case _ => throw new IllegalStateException(s"Unknown command: $command")
    }
    useString + commandString
  }

  def asString(adminCommand: AdministrationCommand): String =  {
    val useString = asString(adminCommand.useGraph)

    def showClausesAsString(yieldOrWhere: YieldOrWhere): (String, String) = {
      val ind: IndentingQueryPrettifier = base.indented()
      yieldOrWhere match {
        case Some(Left((y,r))) => ("\n" + ind.asString(y), r.map(ind.asString).map("\n" + _).getOrElse(""))
        case Some(Right(w)) => ("\n" + ind.asString(w), "")
        case None => ("", "")
      }
    }
    val commandString = adminCommand match {

      // User commands

      case x @ ShowUsers(yields,_) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$y$r"

      case x @ ShowCurrentUser(yields,_) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}$y$r"

      case x @ CreateUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExistsDo) =>
        val userNameString = Prettifier.escapeName(userName)
        val ifNotExists = ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => " IF NOT EXISTS"
          case _                    => ""
        }
        val setPasswordString = if(isEncryptedPassword) "SET ENCRYPTED PASSWORD" else "SET PASSWORD"
        val password = expr.escapePassword(initialPassword)
        val passwordString = s"$setPasswordString $password CHANGE ${if (userOptions.requirePasswordChange.getOrElse(true)) "" else "NOT "}REQUIRED"
        val statusString = if (userOptions.suspended.isDefined) s" SET STATUS ${if (userOptions.suspended.get) "SUSPENDED" else "ACTIVE"}" else ""
        val homeDatabaseString = userOptions.homeDatabase.map {
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeName(name)}"
          case _ => None
        }.getOrElse("")
        s"${x.name} $userNameString$ifNotExists $passwordString$statusString$homeDatabaseString"

      case x @ DropUser(userName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(userName)} IF EXISTS"
        else s"${x.name} ${Prettifier.escapeName(userName)}"

      case x @ AlterUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExists) =>
        val userNameString = Prettifier.escapeName(userName)
        val ifExistsString = if (ifExists) " IF EXISTS" else ""
        val passwordString = initialPassword.map(" " + expr.escapePassword(_)).getOrElse("")
        val passwordModeString = if (userOptions.requirePasswordChange.isDefined)
          s" CHANGE ${if (!userOptions.requirePasswordChange.get) "NOT " else ""}REQUIRED"
        else
          ""
        val setPasswordString = if(isEncryptedPassword.getOrElse(false)) "SET ENCRYPTED PASSWORD" else "SET PASSWORD"
        val passwordPrefix = if (passwordString.nonEmpty || passwordModeString.nonEmpty) s" $setPasswordString" else ""
        val statusString = if (userOptions.suspended.isDefined) s" SET STATUS ${if (userOptions.suspended.get) "SUSPENDED" else "ACTIVE"}" else ""
        val homeDatabaseString = userOptions.homeDatabase.map {
          case SetHomeDatabaseAction(name) => s" SET HOME DATABASE ${Prettifier.escapeName(name)}"
          case RemoveHomeDatabaseAction => " REMOVE HOME DATABASE"
        }.getOrElse("")
        s"${x.name} $userNameString$ifExistsString$passwordPrefix$passwordString$passwordModeString$statusString$homeDatabaseString"

      case x @ SetOwnPassword(newPassword, currentPassword) =>
        s"${x.name} FROM ${expr.escapePassword(currentPassword)} TO ${expr.escapePassword(newPassword)}"

      // Role commands

      case x @ ShowRoles(withUsers, _, yields,_) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"${x.name}${if (withUsers) " WITH USERS" else ""}$y$r"

      case x @ CreateRole(roleName, None, ifExistsDo) =>
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS"
          case _                    => s"${x.name} ${Prettifier.escapeName(roleName)}"
        }

      case x @ CreateRole(roleName, Some(fromRole), ifExistsDo) =>
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS AS COPY OF ${Prettifier.escapeName(fromRole)}"
          case _                    => s"${x.name} ${Prettifier.escapeName(roleName)} AS COPY OF ${Prettifier.escapeName(fromRole)}"
        }

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

      case x @ GrantPrivilege(DbmsPrivilege(_), _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(DbmsPrivilege(_), _, qualifiers, roleNames) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(DbmsPrivilege(_), _, qualifiers, roleNames, _) =>
        s"${x.name}${Prettifier.extractQualifierString(qualifiers)} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

      // database privileges

      case x @ GrantPrivilege(DatabasePrivilege(_, dbScope), _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ DenyPrivilege(DatabasePrivilege(_, dbScope), _, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ RevokePrivilege(DatabasePrivilege(_, dbScope), _, qualifier, roleNames, _) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "FROM", roleNames)

      // graph privileges

      case x @ GrantPrivilege(GraphPrivilege(action, graphScope), resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ DenyPrivilege(GraphPrivilege(action, graphScope), resource, qualifier, roleNames) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "TO", roleNames)

      case x @ RevokePrivilege(GraphPrivilege(action, graphScope), resource, qualifier, roleNames, _) =>
        val qualifierString = Prettifier.prettifyGraphQualifier(action, qualifier)
        Prettifier.prettifyGraphPrivilege(x.name, graphScope, qualifierString, resource, "FROM", roleNames)

      // show privileges

      case ShowPrivileges(scope, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES$y$r"

      case ShowPrivilegeCommands(scope, asRevoke, yields, _) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val asCommand = if (asRevoke) " AS REVOKE COMMANDS" else " AS COMMANDS"
        s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES$asCommand$y$r"

      // Database commands

      case x @ ShowDatabase(scope, yields,_) =>
        val (y: String, r: String) = showClausesAsString(yields)
        val optionalName = scope match {
          case NamedDatabaseScope(dbName) => s" ${Prettifier.escapeName(dbName)}"
          case _ => ""
        }
        s"${x.name}$optionalName$y$r"

      case x @ CreateDatabase(dbName, ifExistsDo, waitUntilComplete) =>
        ifExistsDo match {
          case IfExistsDoNothing | IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(dbName)} IF NOT EXISTS${waitUntilComplete.name}"
          case _                                               => s"${x.name} ${Prettifier.escapeName(dbName)}${waitUntilComplete.name}"
        }

      case x @ DropDatabase(dbName, ifExists, additionalAction, waitUntilComplete) =>
        (ifExists, additionalAction) match {
          case (false, DestroyData) => s"${x.name} ${Prettifier.escapeName(dbName)} DESTROY DATA${waitUntilComplete.name}"
          case (true, DestroyData) => s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DESTROY DATA${waitUntilComplete.name}"
          case (false, DumpData) => s"${x.name} ${Prettifier.escapeName(dbName)} DUMP DATA${waitUntilComplete.name}"
          case (true, DumpData) => s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DUMP DATA${waitUntilComplete.name}"
        }

      case x @ StartDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}${waitUntilComplete.name}"

      case x @ StopDatabase(dbName, waitUntilComplete) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}${waitUntilComplete.name}"
    }
    useString + commandString
  }

  private def asString(use: Option[GraphSelection]) = {
    use.filter(_ => useInCommands).map(u => base.dispatch(u) + NL).getOrElse("")
  }

  case class IndentingQueryPrettifier(indentLevel: Int = 0) extends Prettifier.QueryPrettifier {
    def indented(): IndentingQueryPrettifier = copy(indentLevel + 1)
    val INDENT: String = "  " * indentLevel

    private def asNewLine(l: String) = NL + l

    def query(q: Query): String = {
      val hint = q.periodicCommitHint.map(INDENT + "USING PERIODIC COMMIT" + _.size.map(" " + expr(_)).getOrElse("") + NL).getOrElse("")
      val query = queryPart(q.part)
      s"$hint$query"
    }

    def queryPart(part: QueryPart): String =
      part match {
        case SingleQuery(clauses) =>
          clauses.map(dispatch).mkString(NL)

        case union: Union =>
          val lhs = queryPart(union.part)
          val rhs = queryPart(union.query)
          val operation = union match {
            case _: UnionAll      => s"${INDENT}UNION ALL"
            case _: UnionDistinct => s"${INDENT}UNION"

            case u: ProjectingUnionAll      =>
              s"${INDENT}UNION ALL mappings: (${u.unionMappings.map(asString).mkString(", ")})"
            case u: ProjectingUnionDistinct =>
              s"${INDENT}UNION mappings: (${u.unionMappings.map(asString).mkString(", ")})"
          }
          Seq(lhs, operation, rhs).mkString(NL)
      }

    private def asString(u: UnionMapping): String = {
      s"${u.unionVariable.name}: [${u.variableInPart.name}, ${u.variableInQuery.name}]"
    }

    def asString(clause: Clause): String = dispatch(clause)

    def dispatch(clause: Clause): String = clause match {
      case u: UseGraph          => asString(u)
      case e: Return            => asString(e)
      case m: Match             => asString(m)
      case c: SubQuery          => asString(c)
      case w: With              => asString(w)
      case y: Yield             => asString(y)
      case c: Create            => asString(c)
      case u: Unwind            => asString(u)
      case u: UnresolvedCall    => asString(u)
      case s: ShowIndexesClause => asString(s)
      case s: SetClause         => asString(s)
      case r: Remove            => asString(r)
      case d: Delete            => asString(d)
      case m: Merge             => asString(m)
      case l: LoadCSV           => asString(l)
      case f: Foreach           => asString(f)
      case s: Start             => asString(s)
      case c =>
        val ext = extension.asString(this)
        ext.applyOrElse(c, fallback)
    }

    private def fallback(clause: Clause): String =
      clause.asCanonicalStringVal

    def asString(u: UseGraph): String =
      s"${INDENT}USE ${expr(u.expression)}"

    def asString(m: Match): String = {
      val o = if (m.optional) "OPTIONAL " else ""
      val p = expr.patterns.apply(m.pattern)
      val ind = indented()
      val w = m.where.map(ind.asString).map(asNewLine).getOrElse("")
      val h = m.hints.map(ind.asString).map(asNewLine).mkString
      s"$INDENT${o}MATCH $p$h$w"
    }

    def asString(c: SubQuery): String = {
      s"""${INDENT}CALL {
         |${indented().queryPart(c.part)}
         |$INDENT}""".stripMargin
    }

    def asString(w: Where): String =
      s"${INDENT}WHERE ${expr(w.expression)}"

    def asString(m: UsingHint): String = {
      m match {
        case UsingIndexHint(v, l, ps, s) => Seq(
          s"${INDENT}USING INDEX ", if (s == SeekOnly) "SEEK " else "",
          expr(v), ":", expr(l),
          ps.map(expr(_)).mkString("(", ",", ")")
        ).mkString

        case UsingScanHint(v, l) => Seq(
          s"${INDENT}USING SCAN ", expr(v), ":", expr(l)
        ).mkString

        case UsingJoinHint(vs) => Seq(
          s"${INDENT}USING JOIN ON ", vs.map(expr(_)).toIterable.mkString(", ")
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

    def asString(r: Return): String = {
      val d = if (r.distinct) " DISTINCT" else ""
      val i = asString(r.returnItems)
      val ind = indented()
      val o = r.orderBy.map(ind.asString).map(asNewLine).getOrElse("")
      val l = r.limit.map(ind.asString).map(asNewLine).getOrElse("")
      val s = r.skip.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}RETURN$d $i$o$s$l"
    }

    def asString(w: With): String = {
      val d = if (w.distinct) " DISTINCT" else ""
      val i = asString(w.returnItems)
      val ind = indented()
      val o = w.orderBy.map(ind.asString).map(asNewLine).getOrElse("")
      val l = w.limit.map(ind.asString).map(asNewLine).getOrElse("")
      val s = w.skip.map(ind.asString).map(asNewLine).getOrElse("")
      val wh = w.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}WITH$d $i$o$s$l$wh"
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
      val yields = if (u.yieldAll) asNewLine(s"${indented().INDENT}YIELD *")
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
      val indexType = if (s.all) "ALL" else "BTREE"
      val indexOutput = s match {
        case ShowIndexesClause(_, _, true, _, _, false) => " BRIEF"
        case ShowIndexesClause(_, _, _, true, _, false) => " VERBOSE"
        case _ => ""
      }
      val ind = indented()
      val where = s.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"SHOW $indexType INDEXES$indexOutput$where"
    }

    def asString(s: SetClause): String = {
      val items = s.items.map {
        case SetPropertyItem(prop, exp)                       => s"${expr(prop)} = ${expr(exp)}"
        case SetLabelItem(variable, labels)                   => expr(variable) + labels.map(l => s":${expr(l)}").mkString("")
        case SetIncludingPropertiesFromMapItem(variable, exp) => s"${expr(variable)} += ${expr(exp)}"
        case SetExactPropertiesFromMapItem(variable, exp)     => s"${expr(variable)} = ${expr(exp)}"
        case _                                                => s.asCanonicalStringVal
      }
      s"${INDENT}SET ${items.mkString(", ")}"
    }

    def asString(r: Remove): String = {
      val items = r.items.map {
        case RemovePropertyItem(prop)          => s"${expr(prop)}"
        case RemoveLabelItem(variable, labels) => expr(variable) + labels.map(l => s":${expr(l)}").mkString("")
        case _                                 => r.asCanonicalStringVal
      }
      s"${INDENT}REMOVE ${items.mkString(", ")}"
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

    def asString(start: Start): String = {
      val startItems =
        start.items.map {
          case AllNodes(v)                                               => s"${expr(v)} = NODE( * )"
          case NodeByIds(v, ids)                                         => s"${expr(v)} = NODE( ${ids.map(expr(_)).mkString(", ")} )"
          case NodeByParameter(v, param: Parameter)                      => s"${expr(v)} = NODE( ${expr(param)} )"
          case NodeByParameter(v, param: ParameterWithOldSyntax)         => s"${expr(v)} = NODE( ${expr(param)} )"
          case AllRelationships(v)                                       => s"${expr(v)} = RELATIONSHIP( * )"
          case RelationshipByIds(v, ids)                                 => s"${expr(v)} = RELATIONSHIP( ${ids.map(expr(_)).mkString(", ")} )"
          case RelationshipByParameter(v, param: Parameter)              => s"${expr(v)} = RELATIONSHIP( ${expr(param)} )"
          case RelationshipByParameter(v, param: ParameterWithOldSyntax) => s"${expr(v)} = RELATIONSHIP( ${expr(param)} )"
        }

      val ind = indented()
      val where = start.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}START ${startItems.mkString(s",$NL      ")}$where"
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

  def extractScope(scope: ShowPrivilegeScope): String = {
    scope match {
      case ShowUserPrivileges(name) =>
        if(name.isDefined)
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
      case ShowAllPrivileges()      => "ALL"
      case _                        => "<unknown>"
    }
  }

  def revokeOperation(operation: String, revokeType: String) = s"$operation($revokeType)"

  def prettifyDatabasePrivilege(privilegeName: String,
                                dbScope: List[DatabaseScope],
                                qualifier: List[PrivilegeQualifier],
                                preposition: String,
                                roleNames: Seq[Either[String, Parameter]]): String = {
    val (dbName, default, multiple) = Prettifier.extractDbScope(dbScope)
    val db = if (default) {
      s"$dbName DATABASE"
    } else if (multiple) {
      s"DATABASES $dbName"
    } else {
      s"DATABASE $dbName"
    }
    s"$privilegeName${extractQualifierString(qualifier)} ON $db $preposition ${escapeNames(roleNames)}"
  }

  def prettifyGraphPrivilege(privilegeName: String,
                             graphScope: List[GraphScope],
                             qualifierString: String,
                             resource: Option[ActionResource],
                             preposition: String,
                             roleNames: Seq[Either[String, Parameter]]): String = {

    val resourceName = resource match {
      case Some(PropertyResource(name)) => s" {${ExpressionStringifier.backtick(name)}}"
      case Some(PropertiesResource(names)) => s" {${names.map(ExpressionStringifier.backtick(_)).mkString(", ")}}"
      case Some(AllPropertyResource()) => " {*}"
      case Some(LabelsResource(names)) => s" ${names.map(ExpressionStringifier.backtick(_)).mkString(", ")}"
      case Some(AllLabelResource()) => " *"
      case None => ""
      case _ => throw new IllegalStateException(s"Unknown resource: $resource")
    }
    val scope = s"${extractGraphScope(graphScope)}"
    s"$privilegeName$resourceName ON $scope$qualifierString $preposition ${Prettifier.escapeNames(roleNames)}"
  }

  def prettifyGraphQualifier(action: GraphAction, qualifier: List[PrivilegeQualifier]): String = {
    // For WRITE, we don't want to print out the qualifier. For SET and REMOVE LABEL, it is printed out in another position.
    if (action.name.equals("WRITE") || action.name.equals("SET LABEL") || action.name.equals("REMOVE LABEL")) {
      ""
    } else {
      extractQualifierString(qualifier)
    }
  }

  def extractQualifierPart(qualifier: List[PrivilegeQualifier]): Option[String] = {
    def stringifyQualifiedName(nameSpace: Namespace, name: String) = {
      val namespace = nameSpace.parts.map(ExpressionStringifier.backtick(_, globbing = true)).mkString(".")
      val prefix = if (namespace.isEmpty) "" else namespace + "."
      prefix + ExpressionStringifier.backtick(name, globbing = true)
    }

    def stringify: PartialFunction[PrivilegeQualifier,String] = {
      case LabelQualifier(name) => ExpressionStringifier.backtick(name)
      case RelationshipQualifier(name) => ExpressionStringifier.backtick(name)
      case ElementQualifier(name) => ExpressionStringifier.backtick(name)
      case UserQualifier(name) => escapeName(name)
      case ProcedureQualifier(nameSpace, procedureName) =>
        stringifyQualifiedName(nameSpace, procedureName.name)
      case FunctionQualifier(nameSpace, functionName) =>
        stringifyQualifiedName(nameSpace, functionName.name)
    }

    qualifier match {
      case l@LabelQualifier(_) :: Nil => Some("NODE " + l.map(stringify).mkString(", "))
      case l@LabelQualifier(_) :: _ => Some("NODES " + l.map(stringify).mkString(", "))
      case LabelAllQualifier() :: Nil => Some("NODES *")
      case rels@RelationshipQualifier(_) :: Nil => Some("RELATIONSHIP " + rels.map(stringify).mkString(", "))
      case rels@RelationshipQualifier(_) :: _ => Some("RELATIONSHIPS " + rels.map(stringify).mkString(", "))
      case RelationshipAllQualifier() :: Nil => Some("RELATIONSHIPS *")
      case elems@ElementQualifier(_) :: _ => Some("ELEMENTS " + elems.map(stringify).mkString(", "))
      case ElementsAllQualifier() :: Nil => Some("ELEMENTS *")
      case UserQualifier(user) :: Nil => Some("(" + escapeName(user) + ")")
      case users@UserQualifier(_) :: _ => Some("(" + users.map(stringify).mkString(", ") + ")")
      case UserAllQualifier() :: Nil => Some("(*)")
      case AllQualifier() :: Nil => None
      case AllDatabasesQualifier() :: Nil => None
      case p@ProcedureQualifier(_, _) :: _ => Some(p.map(stringify).mkString(", "))
      case ProcedureAllQualifier() :: Nil => Some("*")
      case p@FunctionQualifier(_, _) :: _ => Some(p.map(stringify).mkString(", "))
      case FunctionAllQualifier() :: Nil => Some("*")
      case _ => Some("<unknown>")
    }
  }

  private def extractQualifierString(qualifier: List[PrivilegeQualifier]): String = {
    val qualifierPart = extractQualifierPart(qualifier)
    qualifierPart match {
      case Some(string) => s" $string"
      case _ => ""
    }
  }

  def extractDbScope(dbScope: List[DatabaseScope]): (String, Boolean, Boolean) = dbScope match {
    case NamedDatabaseScope(name) :: Nil => (escapeName(name), false, false)
    case AllDatabasesScope() :: Nil      => ("*", false, false)
    case DefaultDatabaseScope() :: Nil   => ("DEFAULT", true, false)
    case HomeDatabaseScope() :: Nil      => ("HOME", true, false)
    case namedDatabaseScopes             => (escapeNames(namedDatabaseScopes.collect { case NamedDatabaseScope(name) => name }), false, true)
  }

    def extractGraphScope(graphScope: List[GraphScope]): String = {
      graphScope match {
        case NamedGraphScope(name) :: Nil => s"GRAPH ${escapeName(name)}"
        case AllGraphsScope() :: Nil => "GRAPH *"
        case DefaultGraphScope() :: Nil => "DEFAULT GRAPH"
        case HomeGraphScope() :: Nil => "HOME GRAPH"
        case namedGraphScopes => s"GRAPHS ${escapeNames(namedGraphScopes.collect { case NamedGraphScope(name) => name })}"
      }
  }

  def escapeName(name: Either[String, Parameter]): String = name match {
    case Left(s) => ExpressionStringifier.backtick(s)
    case Right(p) => s"$$${ExpressionStringifier.backtick(p.name)}"
  }

  def escapeNames(names: Seq[Either[String, Parameter]]): String = names.map(escapeName).mkString(", ")

}
