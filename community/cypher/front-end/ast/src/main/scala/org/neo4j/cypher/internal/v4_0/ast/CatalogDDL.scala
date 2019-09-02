/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticCheckResult._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticAnalysisTooling, SemanticCheck, SemanticCheckResult, SemanticError, SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalVariable, Parameter, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols._


sealed trait CatalogDDL extends Statement with SemanticAnalysisTooling {

  def name: String

  override def returnColumns: List[LogicalVariable] = List.empty
}

sealed trait MultiDatabaseAdministrationCommand extends CatalogDDL {
  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position)
}

sealed trait MultiGraphDDL extends CatalogDDL {
  //TODO Refine to split between multigraph and views
  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleGraphs, position)
}

final case class ShowUsers()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = "SHOW USERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateUser(userName: String,
                            initialStringPassword: Option[String],
                            initialParameterPassword: Option[Parameter],
                            requirePasswordChange: Boolean,
                            suspended: Option[Boolean])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(initialStringPassword.isDefined || initialParameterPassword.isDefined)
  assert(!(initialStringPassword.isDefined && initialParameterPassword.isDefined))

  override def name = "CREATE USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropUser(userName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterUser(userName: String,
                           initialStringPassword: Option[String],
                           initialParameterPassword: Option[Parameter],
                           requirePasswordChange: Option[Boolean],
                           suspended: Option[Boolean])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(initialStringPassword.isDefined || initialParameterPassword.isDefined || requirePasswordChange.isDefined || suspended.isDefined)
  assert(!(initialStringPassword.isDefined && initialParameterPassword.isDefined))

  override def name = "ALTER USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class SetOwnPassword(newStringPassword: Option[String],
                                newParameterPassword: Option[Parameter],
                                currentStringPassword: Option[String],
                                currentParameterPassword: Option[Parameter])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(newStringPassword.isDefined || newParameterPassword.isDefined)
  assert(!(newStringPassword.isDefined && newParameterPassword.isDefined))
  assert(currentStringPassword.isDefined || currentParameterPassword.isDefined)
  assert(!(currentStringPassword.isDefined && currentParameterPassword.isDefined))

  override def name = "ALTER CURRENT USER SET PASSWORD"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowRoles(withUsers: Boolean, showAll: Boolean)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateRole(roleName: String, from: Option[String])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "CREATE ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropRole(roleName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantRolesToUsers(roleNames: Seq[String], userNames: Seq[String])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "GRANT ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class RevokeRolesFromUsers(roleNames: Seq[String], userNames: Seq[String])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "REVOKE ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

abstract class PrivilegeType(val name: String)

final case class TraversePrivilege()(val position: InputPosition) extends PrivilegeType("TRAVERSE")

final case class ReadPrivilege()(val position: InputPosition) extends PrivilegeType("READ")

final case class MatchPrivilege()(val position: InputPosition) extends PrivilegeType("MATCH")

final case class WritePrivilege()(val position: InputPosition) extends PrivilegeType("WRITE")

abstract class RevokeType(val name: String, val relType: String)

final case class RevokeGrantType()(val position: InputPosition) extends RevokeType("GRANT", "GRANTED")

final case class RevokeDenyType()(val position: InputPosition) extends RevokeType("DENY", "DENIED")

final case class RevokeBothType()(val position: InputPosition) extends RevokeType("", "")

sealed trait ActionResource {
  def simplify: Seq[ActionResource] = Seq(this)
}

final case class PropertyResource(property: String)(val position: InputPosition) extends ActionResource

final case class PropertiesResource(properties: Seq[String])(val position: InputPosition) extends ActionResource {
  override def simplify: Seq[ActionResource] = properties.map(PropertyResource(_)(position))
}

final case class AllResource()(val position: InputPosition) extends ActionResource

final case class NoResource()(val position: InputPosition) extends ActionResource

sealed trait PrivilegeQualifier {
  def simplify: Seq[PrivilegeQualifier] = Seq(this)
}

final case class LabelQualifier(label: String)(val position: InputPosition) extends PrivilegeQualifier

final case class LabelsQualifier(labels: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = labels.map(LabelQualifier(_)(position))
}

final case class RelationshipQualifier(reltype: String)(val position: InputPosition) extends PrivilegeQualifier

final case class RelationshipsQualifier(reltypes: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = reltypes.map(RelationshipQualifier(_)(position))
}

final case class ElementsQualifier(values: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = values.map(LabelQualifier(_)(position)) ++ values.map(RelationshipQualifier(_)(position))
}

final case class AllQualifier()(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = Seq(LabelAllQualifier()(position), RelationshipAllQualifier()(position))
}

final case class LabelAllQualifier()(val position: InputPosition) extends PrivilegeQualifier

final case class RelationshipAllQualifier()(val position: InputPosition) extends PrivilegeQualifier

sealed trait GraphScope

final case class NamedGraphScope(database: String)(val position: InputPosition) extends GraphScope

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope

sealed trait ShowPrivilegeScope

final case class ShowRolePrivileges(role: String)(val position: InputPosition) extends ShowPrivilegeScope

final case class ShowUserPrivileges(user: String)(val position: InputPosition) extends ShowPrivilegeScope

final case class ShowAllPrivileges()(val position: InputPosition) extends ShowPrivilegeScope

object GrantPrivilege {
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def write(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(WritePrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
}

object DenyPrivilege {
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def write(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(WritePrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
}

object RevokePrivilege {
  // Revoke of grant
  def grantedTraverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedRead(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedAsMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedWrite(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))

  // Revoke of deny
  def deniedTraverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedRead(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedAsMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedWrite(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))

  // Revoke
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames,RevokeBothType()(InputPosition.NONE))
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
  def write(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
}

final case class GrantPrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String])
                               (val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = s"GRANT ${privilege.name}"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DenyPrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String])
                                (val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = s"DENY ${privilege.name}"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class RevokePrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String],
                                 revokeType: RevokeType)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = {
    if (revokeType.name.nonEmpty) {
      s"REVOKE ${revokeType.name} ${privilege.name}"
    } else {
      s"REVOKE ${privilege.name}"
    }
  }

  override def semanticCheck: SemanticCheck = privilege match {
    case _: MatchPrivilege =>
      if (revokeType.name.nonEmpty) {
        SemanticError(s"$name is not a valid command, use REVOKE ${revokeType.name} READ and REVOKE ${revokeType.name} TRAVERSE instead.", position)
      } else {
        SemanticError(s"$name is not a valid command, use REVOKE READ and REVOKE TRAVERSE instead.", position)
      }
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

final case class ShowPrivileges(scope: ShowPrivilegeScope)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDatabases()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DATABASES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDefaultDatabase()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DEFAULT DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "CREATE DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StartDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

object CreateGraph {
  def apply(graphName: CatalogName, query: Query)(position: InputPosition): CreateGraph =
    CreateGraph(graphName, query.part)(position)
}

final case class CreateGraph(graphName: CatalogName, query: QueryPart)
  (val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG CREATE GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      query.semanticCheck
}

final case class DropGraph(graphName: CatalogName)(val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG DROP GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

object CreateView {
  def apply(graphName: CatalogName, params: Seq[Parameter], query: Query, innerQString: String)(position: InputPosition): CreateView =
    CreateView(graphName, params, query.part, innerQString)(position)
}

final case class CreateView(graphName: CatalogName, params: Seq[Parameter], query: QueryPart, innerQString: String)
  (val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG CREATE VIEW/QUERY"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      recordGraphParameters chain
      query.semanticCheck

  private def recordGraphParameters(state: SemanticState): SemanticCheckResult = {
    params.foldLeft(success(state): SemanticCheckResult) { case (SemanticCheckResult(s, errors), p) =>
      s.declareVariable(Variable(s"$$${p.name}")(position), CTGraphRef) match {
        case Right(updatedState) => success(updatedState)
        case Left(semanticError) => SemanticCheckResult(s, errors :+ semanticError)
      }
    }
  }

}

final case class DropView(graphName: CatalogName)(val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG DROP VIEW/QUERY"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
