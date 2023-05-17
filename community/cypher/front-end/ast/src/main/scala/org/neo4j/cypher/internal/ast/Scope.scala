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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable

sealed trait GraphOrDatabaseScope extends Rewritable {
  override def dup(children: Seq[AnyRef]): GraphOrDatabaseScope.this.type = this
}

sealed trait HomeScope

sealed trait AllScope

// Graph scopes

sealed trait GraphScope extends GraphOrDatabaseScope

final case class NamedGraphScope(graph: DatabaseName)(val position: InputPosition)
    extends GraphScope {

  override def dup(children: Seq[AnyRef]): NamedGraphScope.this.type =
    this.copy(children.head.asInstanceOf[DatabaseName])(position).asInstanceOf[this.type]
}

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope with AllScope

final case class DefaultGraphScope()(val position: InputPosition) extends GraphScope with HomeScope

final case class HomeGraphScope()(val position: InputPosition) extends GraphScope with HomeScope

// Database scopes

sealed trait DatabaseScope extends GraphOrDatabaseScope {
  val showCommandName: String
}

final case class NamedDatabaseScope(database: DatabaseName)(val position: InputPosition)
    extends DatabaseScope {

  override def dup(children: Seq[AnyRef]): NamedDatabaseScope.this.type =
    this.copy(children.head.asInstanceOf[DatabaseName])(position).asInstanceOf[this.type]

  override val showCommandName: String = "ShowDatabase"
}

final case class AllDatabasesScope()(val position: InputPosition) extends DatabaseScope with AllScope {
  override val showCommandName: String = "ShowDatabases"
}

final case class DefaultDatabaseScope()(val position: InputPosition) extends DatabaseScope with HomeScope {
  override val showCommandName: String = "ShowDefaultDatabase"
}

final case class HomeDatabaseScope()(val position: InputPosition) extends DatabaseScope with HomeScope {
  override val showCommandName: String = "ShowHomeDatabase"
}

// Dbms scopes

sealed trait ShowPrivilegeScope extends Rewritable {
  override def dup(children: Seq[AnyRef]): ShowPrivilegeScope.this.type = this
}

final case class ShowRolesPrivileges(roles: List[Either[String, Parameter]])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowRolesPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowUserPrivileges(user: Option[Either[String, Parameter]])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowUserPrivileges.this.type =
    this.copy(children.head.asInstanceOf[Option[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowUsersPrivileges(users: List[Either[String, Parameter]])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowUsersPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowAllPrivileges()(val position: InputPosition) extends ShowPrivilegeScope
