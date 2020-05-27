/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ast.CreateGraph
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexNewSyntax
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateView
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropGraph
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropView
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.ElementsQualifier
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FromGraph
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelsQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.MultiGraphDDL
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NodeByIds
import org.neo4j.cypher.internal.ast.NodeByParameter
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
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
import org.neo4j.cypher.internal.ast.RelationshipsQualifier
import org.neo4j.cypher.internal.ast.Remove
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
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowDatabases
import org.neo4j.cypher.internal.ast.ShowDefaultDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRolePrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Start
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.TraversePrivilege
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.UsersQualifier
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.LabelName
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
    case c: MultiGraphDDL         => asString(c)
  }

  def asString(command: SchemaCommand): String = {
    def backtick(s: String) = ExpressionStringifier.backtick(s)
    def propertiesToString(properties: Seq[Property]): String = properties.map(propertyToString).mkString("(", ", ", ")")
    def propertyToString(property: Property): String = s"${expr(property.map)}.${ExpressionStringifier.backtick(property.propertyKey.name)}"

    val useString = asString(command.useGraph)
    val commandString = command match {

      case CreateIndex(LabelName(label), properties, _) =>
        s"CREATE INDEX ON :${backtick(label)}${properties.map(p => backtick(p.name)).mkString("(", ", ", ")")}"

      case CreateIndexNewSyntax(Variable(variable), LabelName(label), properties, name, _) =>
        val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
        s"CREATE INDEX ${nameString}FOR (${backtick(variable)}:${backtick(label)}) ON ${propertiesToString(properties)}"

      case DropIndex(LabelName(label), properties, _) =>
        s"DROP INDEX ON :${backtick(label)}${properties.map(p => backtick(p.name)).mkString("(", ", ", ")")}"

      case DropIndexOnName(name, _) =>
        s"DROP INDEX ${backtick(name)}"

      case CreateNodeKeyConstraint(Variable(variable), LabelName(label), properties, name, _) =>
        val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
        s"CREATE CONSTRAINT ${nameString}ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS NODE KEY"

      case DropNodeKeyConstraint(Variable(variable), LabelName(label), properties, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS NODE KEY"

      case CreateUniquePropertyConstraint(Variable(variable), LabelName(label), properties, name, _) =>
        val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
        s"CREATE CONSTRAINT ${nameString}ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS UNIQUE"

      case DropUniquePropertyConstraint(Variable(variable), LabelName(label), properties, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT ${propertiesToString(properties)} IS UNIQUE"

      case CreateNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, name, _) =>
        val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
        s"CREATE CONSTRAINT ${nameString}ON (${backtick(variable)}:${backtick(label)}) ASSERT exists(${propertyToString(property)})"

      case DropNodePropertyExistenceConstraint(Variable(variable), LabelName(label), property, _) =>
        s"DROP CONSTRAINT ON (${backtick(variable)}:${backtick(label)}) ASSERT exists(${propertyToString(property)})"

      case CreateRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, name, _) =>
        val nameString = name.map(n => s"${backtick(n)} ").getOrElse("")
        s"CREATE CONSTRAINT ${nameString}ON ()-[${backtick(variable)}:${backtick(relType)}]-() ASSERT exists(${propertyToString(property)})"

      case DropRelationshipPropertyExistenceConstraint(Variable(variable), RelTypeName(relType), property, _) =>
        s"DROP CONSTRAINT ON ()-[${backtick(variable)}:${backtick(relType)}]-() ASSERT exists(${propertyToString(property)})"

      case DropConstraintOnName(name, _) =>
        s"DROP CONSTRAINT ${backtick(name)}"
    }
    useString + commandString
  }

  def asString(adminCommand: AdministrationCommand): String =  {
    val useString = asString(adminCommand.useGraph)

    def showClausesAsString(yields: Option[Return],
                      where: Option[Where],
                      returns: Option[Return]): (String, String, String) = {
      val ind: IndentingQueryPrettifier = base.indented()
      val w = where.map(ind.asString).map("\n" + _).getOrElse("")
      val y = yields.map(ind.asString).map("\n" + _.replace("RETURN", "YIELD")).getOrElse("")
      val r = returns.map(ind.asString).map("\n" + _).getOrElse("")
      (w, y, r)
    }
    val commandString = adminCommand match {

      case x @ ShowUsers(yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"${x.name}$y$w$r"

      case x @ CreateUser(userName, initialPassword, requirePasswordChange, suspended, ifExistsDo) =>
        val userNameString = Prettifier.escapeName(userName)
        val ifNotExists = ifExistsDo match {
          case _: IfExistsDoNothing | _: IfExistsInvalidSyntax => " IF NOT EXISTS"
          case _                    => ""
        }
        val password = expr.escapePassword(initialPassword)
        val passwordString = s"SET PASSWORD $password CHANGE ${if (!requirePasswordChange) "NOT " else ""}REQUIRED"
        val statusString = if (suspended.isDefined) s" SET STATUS ${if (suspended.get) "SUSPENDED" else "ACTIVE"}"
        else ""
        s"${x.name} $userNameString$ifNotExists $passwordString$statusString"

      case x @ DropUser(userName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(userName)} IF EXISTS"
        else s"${x.name} ${Prettifier.escapeName(userName)}"

      case x @ AlterUser(userName, initialPassword, requirePasswordChange, suspended) =>
        val userNameString = Prettifier.escapeName(userName)
        val passwordString = initialPassword.map(" " + expr.escapePassword(_)).getOrElse("")
        val passwordModeString = if (requirePasswordChange.isDefined)
          s" CHANGE ${if (!requirePasswordChange.get) "NOT " else ""}REQUIRED"
        else
          ""
        val passwordPrefix = if (passwordString.nonEmpty || passwordModeString.nonEmpty) " SET PASSWORD" else ""
        val statusString = if (suspended.isDefined) s" SET STATUS ${if (suspended.get) "SUSPENDED" else "ACTIVE"}" else ""
        s"${x.name} $userNameString$passwordPrefix$passwordString$passwordModeString$statusString"

      case x @ SetOwnPassword(newPassword, currentPassword) =>
        s"${x.name} FROM ${expr.escapePassword(currentPassword)} TO ${expr.escapePassword(newPassword)}"

      case x @ ShowRoles(withUsers, _, yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"${x.name}${if (withUsers) " WITH USERS" else ""}$y$w$r"

      case x @ CreateRole(roleName, None, ifExistsDo) =>
        ifExistsDo match {
          case _: IfExistsDoNothing | _: IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS"
          case _                    => s"${x.name} ${Prettifier.escapeName(roleName)}"
        }

      case x @ CreateRole(roleName, Some(fromRole), ifExistsDo) =>
        ifExistsDo match {
          case _: IfExistsDoNothing | _: IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(roleName)} IF NOT EXISTS AS COPY OF ${Prettifier.escapeName(fromRole)}"
          case _                    => s"${x.name} ${Prettifier.escapeName(roleName)} AS COPY OF ${Prettifier.escapeName(fromRole)}"
        }

      case x @ DropRole(roleName, ifExists) =>
        if (ifExists) s"${x.name} ${Prettifier.escapeName(roleName)} IF EXISTS"
        else s"${x.name} ${Prettifier.escapeName(roleName)}"

      case x @ GrantRolesToUsers(roleNames, userNames) if roleNames.length > 1 =>
        s"${x.name}S ${roleNames.map(Prettifier.escapeName).mkString(", ")} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ GrantRolesToUsers(roleNames, userNames) =>
        s"${x.name} ${roleNames.map(Prettifier.escapeName).mkString(", ")} TO ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ RevokeRolesFromUsers(roleNames, userNames) if roleNames.length > 1 =>
        s"${x.name}S ${roleNames.map(Prettifier.escapeName).mkString(", ")} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ RevokeRolesFromUsers(roleNames, userNames) =>
        s"${x.name} ${roleNames.map(Prettifier.escapeName).mkString(", ")} FROM ${userNames.map(Prettifier.escapeName).mkString(", ")}"

      case x @ GrantPrivilege(DbmsPrivilege(_), _, _, _, roleNames) =>
        s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(DbmsPrivilege(_), _, _, _, roleNames) =>
        s"${x.name} ON DBMS TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(DbmsPrivilege(_), _, _, _, roleNames, _) =>
        s"${x.name} ON DBMS FROM ${Prettifier.escapeNames(roleNames)}"

      case x @ GrantPrivilege(DatabasePrivilege(_), _, dbScope, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ DenyPrivilege(DatabasePrivilege(_), _, dbScope, qualifier, roleNames) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "TO", roleNames)

      case x @ RevokePrivilege(DatabasePrivilege(_), _, dbScope, qualifier, roleNames, _) =>
        Prettifier.prettifyDatabasePrivilege(x.name, dbScope, qualifier, "FROM", roleNames)

      case x@GrantPrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@DenyPrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@RevokePrivilege(TraversePrivilege(), _, dbScope, qualifier, roleNames, _) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope FROM ${Prettifier.escapeNames(roleNames)}"

      case x@GrantPrivilege(GraphPrivilege(WriteAction), _, dbScope, _, roleNames) =>
        val scope = Prettifier.extractGraphScope(dbScope)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@DenyPrivilege(GraphPrivilege(WriteAction), _, dbScope, _, roleNames) =>
        val scope = Prettifier.extractGraphScope(dbScope)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@RevokePrivilege(GraphPrivilege(WriteAction), _, dbScope, _, roleNames, _) =>
        val scope = Prettifier.extractGraphScope(dbScope)
        s"${x.name} ON $scope FROM ${Prettifier.escapeNames(roleNames)}"

      case x@GrantPrivilege(GraphPrivilege(_), None, dbScope, qualifier, roleNames) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@DenyPrivilege(GraphPrivilege(_), None, dbScope, qualifier, roleNames) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@RevokePrivilege(GraphPrivilege(_), None, dbScope, qualifier, roleNames, _) =>
        val scope = Prettifier.extractScope(dbScope, qualifier)
        s"${x.name} ON $scope FROM ${Prettifier.escapeNames(roleNames)}"

      case x@GrantPrivilege(GraphPrivilege(_), Some(resource), dbScope, _, roleNames)
        if resource.isInstanceOf[LabelsResource] || resource.isInstanceOf[AllLabelResource] =>
          val scope = Prettifier.extractLabelScope(dbScope, resource)
          s"${x.name} $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@DenyPrivilege(GraphPrivilege(_), Some(resource), dbScope, _, roleNames)
        if resource.isInstanceOf[LabelsResource] || resource.isInstanceOf[AllLabelResource] =>
          val scope = Prettifier.extractLabelScope(dbScope, resource)
          s"${x.name} $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x@RevokePrivilege(GraphPrivilege(_), Some(resource), dbScope, _, roleNames, _)
        if resource.isInstanceOf[LabelsResource] || resource.isInstanceOf[AllLabelResource] =>
          val scope = Prettifier.extractLabelScope(dbScope, resource)
          s"${x.name} $scope FROM ${Prettifier.escapeNames(roleNames)}"

      case x @ GrantPrivilege(_, Some(resource), dbScope, qualifier, roleNames) =>
        val (resourceName, scope) = Prettifier.extractScope(resource, dbScope, qualifier)
        s"${x.name} {$resourceName} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x @ DenyPrivilege(_, Some(resource), dbScope, qualifier, roleNames) =>
        val (resourceName, scope) = Prettifier.extractScope(resource, dbScope, qualifier)
        s"${x.name} {$resourceName} ON $scope TO ${Prettifier.escapeNames(roleNames)}"

      case x @ RevokePrivilege(_, Some(resource), dbScope, qualifier, roleNames, _) =>
        val (resourceName, scope) = Prettifier.extractScope(resource, dbScope, qualifier)
        s"${x.name} {$resourceName} ON $scope FROM ${Prettifier.escapeNames(roleNames)}"

      case ShowPrivileges(scope, yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"SHOW ${Prettifier.extractScope(scope)} PRIVILEGES$y$w$r"

      case x @ ShowDatabases(yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"${x.name}$y$w$r"

      case x @ ShowDefaultDatabase(yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"${x.name}$y$w$r"

      case x @ ShowDatabase(dbName, yields, where, returns) =>
        val (w: String, y: String, r: String) = showClausesAsString(yields, where, returns)
        s"${x.name} ${Prettifier.escapeName(dbName)}$y$w$r"

      case x @ CreateDatabase(dbName, ifExistsDo) =>
        ifExistsDo match {
          case _: IfExistsDoNothing | _: IfExistsInvalidSyntax => s"${x.name} ${Prettifier.escapeName(dbName)} IF NOT EXISTS"
          case _                                               => s"${x.name} ${Prettifier.escapeName(dbName)}"
        }

      case x @ DropDatabase(dbName, ifExists, additionalAction) =>
        (ifExists, additionalAction) match {
          case (false, DestroyData) => s"${x.name} ${Prettifier.escapeName(dbName)} DESTROY DATA"
          case (true, DestroyData) => s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DESTROY DATA"
          case (false, DumpData) => s"${x.name} ${Prettifier.escapeName(dbName)} DUMP DATA"
          case (true, DumpData) => s"${x.name} ${Prettifier.escapeName(dbName)} IF EXISTS DUMP DATA"
        }

      case x @ StartDatabase(dbName) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}"

      case x @ StopDatabase(dbName) =>
        s"${x.name} ${Prettifier.escapeName(dbName)}"
    }
    useString + commandString
  }

  def asString(multiGraph: MultiGraphDDL): String = multiGraph match {
    case x @ CreateGraph(catalogName, query) =>
      val graphName = catalogName.parts.mkString(".")
      s"${x.name} $graphName {$NL${base.indented().queryPart(query)}$NL}"

    case x @ DropGraph(catalogName) =>
      val graphName = catalogName.parts.mkString(".")
      s"${x.name} $graphName"

    case CreateView(catalogName, params, query, _) =>
      val graphName = catalogName.parts.mkString(".")
      val paramString = params.map(p => "$" + p.name).mkString("(", ", ", ")")
      s"CATALOG CREATE VIEW $graphName$paramString {$NL${base.indented().queryPart(query)}$NL}"

    case DropView(catalogName) =>
      val graphName = catalogName.parts.mkString(".")
      s"CATALOG DROP VIEW $graphName"
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
      case u: UseGraph       => asString(u)
      case f: FromGraph      => asString(f)
      case e: Return         => asString(e)
      case m: Match          => asString(m)
      case c: SubQuery       => asString(c)
      case w: With           => asString(w)
      case c: Create         => asString(c)
      case u: Unwind         => asString(u)
      case u: UnresolvedCall => asString(u)
      case s: SetClause      => asString(s)
      case r: Remove         => asString(r)
      case d: Delete         => asString(d)
      case m: Merge          => asString(m)
      case l: LoadCSV        => asString(l)
      case f: Foreach        => asString(f)
      case s: Start          => asString(s)
      case c =>
        val ext = extension.asString(this)
        ext.applyOrElse(c, fallback)
    }

    private def fallback(clause: Clause): String =
      clause.asCanonicalStringVal

    def asString(u: UseGraph): String =
      s"${INDENT}USE ${expr(u.expression)}"

    def asString(f: FromGraph): String =
      s"${INDENT}FROM ${expr(f.expression)}"

    def asString(m: Match): String = {
      val o = if (m.optional) "OPTIONAL " else ""
      val p = expr.patterns.apply(m.pattern)
      val ind = indented()
      val w = m.where.map(ind.asString).map(asNewLine).getOrElse("")
      val h = m.hints.map(ind.asString).map(asNewLine).mkString
      s"${INDENT}${o}MATCH $p$h$w"
    }

    def asString(c: SubQuery): String = {
      s"""${INDENT}CALL {
         |${indented().queryPart(c.part)}
         |${INDENT}}""".stripMargin
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
      val yields = u.declaredResult.filter(_.items.nonEmpty).map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}CALL $prefix${expr(u.procedureName)}$arguments$yields"
    }

    def asString(r: ProcedureResult): String = {
      def item(i: ProcedureResultItem) = i.output.map(expr(_) + " AS ").getOrElse("") + expr(i.variable)
      val items = r.items.map(item).mkString(", ")
      val ind = indented()
      val where = r.where.map(ind.asString).map(asNewLine).getOrElse("")
      s"${INDENT}YIELD $items$where"
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
      s"${INDENT}${detach}DELETE ${delete.expressions.map(expr(_)).mkString(", ")}"
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
      case ShowRolePrivileges(name) => s"ROLE ${escapeName(name)}"
      case ShowAllPrivileges()      => "ALL"
      case _                        => "<unknown>"
    }
  }

  def extractGraphScope(dbScope: List[GraphScope]): String = {
    val (dbString, _, multipleDbs) = extractDbScope(dbScope)
    val graphWord = if (multipleDbs) "GRAPHS" else "GRAPH"
    s"$graphWord $dbString"
  }

  def extractScope(dbScope: List[GraphScope], qualifier: PrivilegeQualifier): String = {
    s"${extractGraphScope(dbScope)}${extractQualifierString(qualifier)}"
  }

  def extractLabelScope(dbScope: List[GraphScope], resource: ActionResource): String = {
    val labelNames = resource match {
      case LabelsResource(names) => names.map(ExpressionStringifier.backtick(_)).mkString(", ")
      case AllLabelResource() => "*"
    }
    val (dbString, _, multipleDbs) = extractDbScope(dbScope)
    val graphWord = if (multipleDbs) "GRAPHS" else "GRAPH"
    s"$labelNames ON $graphWord $dbString"
  }

  def extractScope(resource: ActionResource, dbScope: List[GraphScope], qualifier: PrivilegeQualifier): (String, String) = {
    val resourceName = resource match {
      case PropertyResource(name) => ExpressionStringifier.backtick(name)
      case PropertiesResource(names) => names.map(ExpressionStringifier.backtick(_)).mkString(", ")
      case AllPropertyResource() => "*"
      case _ => "<unknown>"
    }
    (resourceName, extractScope(dbScope, qualifier))
  }

  def revokeOperation(operation: String, revokeType: String) = s"$operation($revokeType)"

  def prettifyDatabasePrivilege(privilegeName: String,
                                dbScope: List[GraphScope],
                                qualifier: PrivilegeQualifier,
                                preposition: String,
                                roleNames: Seq[Either[String, Parameter]]): String = {
    val (dbName, default, multiple) = Prettifier.extractDbScope(dbScope)
    val db = if (default) {
      s"DEFAULT DATABASE"
    } else if (multiple) {
      s"DATABASES $dbName"
    } else {
      s"DATABASE $dbName"
    }
    s"$privilegeName${extractQualifierString(qualifier)} ON $db $preposition ${escapeNames(roleNames)}"
  }

  def extractQualifierPart(qualifier: PrivilegeQualifier): Option[String] = qualifier match {
    case LabelQualifier(name)          => Some("NODE " + ExpressionStringifier.backtick(name))
    case LabelsQualifier(names)        => Some("NODES " + names.map(ExpressionStringifier.backtick(_)).mkString(", "))
    case LabelAllQualifier()           => Some("NODES *")
    case RelationshipQualifier(name)   => Some("RELATIONSHIP " + ExpressionStringifier.backtick(name))
    case RelationshipsQualifier(names) => Some("RELATIONSHIPS " + names.map(ExpressionStringifier.backtick(_)).mkString(", "))
    case RelationshipAllQualifier()    => Some("RELATIONSHIPS *")
    case ElementsQualifier(names)      => Some("ELEMENTS " + names.map(ExpressionStringifier.backtick(_)).mkString(", "))
    case ElementsAllQualifier()        => Some("ELEMENTS *")
    case UsersQualifier(names)         => Some("(" + names.map(escapeName).mkString(", ") + ")")
    case UserQualifier(name)           => Some("(" + escapeName(name) + ")")
    case UserAllQualifier()            => Some("(*)")
    case AllQualifier()                => None
    case _                             => Some("<unknown>")
  }

  private def extractQualifierString(qualifier: PrivilegeQualifier): String = {
    val qualifierPart = extractQualifierPart(qualifier)
    qualifierPart match {
      case Some(string) => s" $string"
      case _ => ""
    }
  }

  def extractDbScope(dbScope: List[GraphScope]): (String, Boolean, Boolean) = dbScope match {
    case NamedGraphScope(name) :: Nil => (escapeName(name), false, false)
    case AllGraphsScope() :: Nil => ("*", false, false)
    case DefaultDatabaseScope() :: Nil => ("DEFAULT", true, false)
    case namedGraphScopes => (escapeNames(namedGraphScopes.collect { case NamedGraphScope(name) => name }), false, true)
  }

  def escapeName(name: Either[String, Parameter]): String = name match {
    case Left(s) => ExpressionStringifier.backtick(s)
    case Right(p) => s"$$${ExpressionStringifier.backtick(p.name)}"
  }

  def escapeNames(names: Seq[Either[String, Parameter]]): String = names.map(escapeName).mkString(", ")

}
