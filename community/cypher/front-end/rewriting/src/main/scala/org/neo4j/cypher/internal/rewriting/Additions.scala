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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.CreateIndexNewSyntax
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DbmsAdminAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RoleManagementAction
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowDatabases
import org.neo4j.cypher.internal.ast.ShowDefaultDatabase
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TransactionManagementAction
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.util.CypherExceptionFactory

object Additions {

  // This is functionality that has been added in 4.0 and should not work when using CYPHER 3.5
  case object addedFeaturesIn4_0 extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {

      // CREATE INDEX [name] FOR (n:Label) ON (n.prop)
      case c: CreateIndexNewSyntax =>
        throw cypherExceptionFactory.syntaxException("Creating index using this syntax is not supported in this Cypher version.", c.position)

      // DROP INDEX name
      case d: DropIndexOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping index by name is not supported in this Cypher version.", d.position)

      // CREATE CONSTRAINT name ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, Some(_), _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node key constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, Some(_), _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named uniqueness constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, Some(_), _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, Some(_), _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named relationship existence constraint is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name
      case d: DropConstraintOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint by name is not supported in this Cypher version.", d.position)

      case e: ExistsSubClause =>
        throw cypherExceptionFactory.syntaxException("Existential subquery is not supported in this Cypher version.", e.position)

      // Administration commands against system database are checked in CompilerFactory to cover all of them at once
    }
  }

  // This is functionality that has been added in 4.1 and should not work when using CYPHER 3.5 and CYPHER 4.0
  case object addedFeaturesIn4_1 extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {

      // Grant DEFAULT DATABASE
      case p@GrantPrivilege(_, _, List(DefaultDatabaseScope()), _, _) =>
        throw cypherExceptionFactory.syntaxException("DEFAULT DATABASE is not supported in this Cypher version.", p.position)

      // Deny DEFAULT DATABASE
      case p@DenyPrivilege(_, _, List(DefaultDatabaseScope()), _, _) =>
        throw cypherExceptionFactory.syntaxException("DEFAULT DATABASE is not supported in this Cypher version.", p.position)

      // Revoke DEFAULT DATABASE
      case p@RevokePrivilege(_, _, List(DefaultDatabaseScope()), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("DEFAULT DATABASE is not supported in this Cypher version.", p.position)

      // grant dbms privilege (except role management)
      case p@GrantPrivilege(DbmsPrivilege(action: DbmsAdminAction), _, _, _, _) if !action.isInstanceOf[RoleManagementAction] =>
        throw cypherExceptionFactory.syntaxException(s"${action.name} privilege is not supported in this Cypher version.", p.position)

      // deny dbms privilege (except role management)
      case p@DenyPrivilege(DbmsPrivilege(action: DbmsAdminAction), _, _, _, _) if !action.isInstanceOf[RoleManagementAction] =>
        throw cypherExceptionFactory.syntaxException(s"${action.name} privilege is not supported in this Cypher version.", p.position)

      // revoke dbms privilege (except role management)
      case p@RevokePrivilege(DbmsPrivilege(action: DbmsAdminAction), _, _, _, _, _) if !action.isInstanceOf[RoleManagementAction] =>
        throw cypherExceptionFactory.syntaxException(s"${action.name} privilege is not supported in this Cypher version.", p.position)

      // grant transaction administration
      case p@GrantPrivilege(DatabasePrivilege(_: TransactionManagementAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Transaction administration privileges are not supported in this Cypher version.", p.position)

      // deny transaction administration
      case p@DenyPrivilege(DatabasePrivilege(_: TransactionManagementAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Transaction administration privileges are not supported in this Cypher version.", p.position)

      // revoke transaction administration
      case p@RevokePrivilege(DatabasePrivilege(_: TransactionManagementAction), _, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Transaction administration privileges are not supported in this Cypher version.", p.position)

      // grant fine-grained write
      case p@GrantPrivilege(GraphPrivilege(action), _, _, _, _) if !action.equals(WriteAction) =>
        throw cypherExceptionFactory.syntaxException(errorMessage(action), p.position)

      // deny fine-grained write
      case p@DenyPrivilege(GraphPrivilege(action), _, _, _, _) if !action.equals(WriteAction) =>
        throw cypherExceptionFactory.syntaxException(errorMessage(action), p.position)

      // revoke fine-grained
      case p@RevokePrivilege(GraphPrivilege(action), _, _, _, _, _) if !action.equals(WriteAction) =>
        throw cypherExceptionFactory.syntaxException(errorMessage(action), p.position)

      // remove database dump data
      case p@DropDatabase(_,_,DumpData) =>
        throw cypherExceptionFactory.syntaxException("Dumping data when dropping databases is not supported in this Cypher version.", p.position)

      // extended show commands
      case sp @ ShowPrivileges(_, yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", sp.position)
      case sr @ ShowRoles(_, _, yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", sr.position)
      case su @ ShowUsers(yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", su.position)
      case sd @ ShowDatabases(yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", sd.position)
      case sd @ ShowDatabase(_, yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", sd.position)
      case sd @ ShowDefaultDatabase(yields, where, returns) if Seq(yields, where, returns).flatten.nonEmpty =>
        throw cypherExceptionFactory.syntaxException("Extended show commands are not supported in this Cypher version.", sd.position)

      // CREATE INDEX [name] IF NOT EXISTS ...
      case c@CreateIndexNewSyntax(_, _, _, _, IfExistsDoNothing(), _) =>
        throw cypherExceptionFactory.syntaxException("Creating index using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // DROP INDEX name IF EXISTS
      case d@DropIndexOnName(_, true, _) =>
        throw cypherExceptionFactory.syntaxException("Dropping index using `IF EXISTS` is not supported in this Cypher version.", d.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, _, IfExistsDoNothing(), _) =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, _, IfExistsDoNothing(), _) =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing(), _) =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing(), _) =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name IF EXISTS
      case d@DropConstraintOnName(_, true, _) =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint using `IF EXISTS` is not supported in this Cypher version.", d.position)
    }
  }

  private def errorMessage(action: GraphAction) = {
    val prefix = if (action.equals(AllGraphAction)) s"${action.name} is" else "Fine-grained writes are"
    s"$prefix not supported in this Cypher version."
  }
}

trait Additions extends {
  def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit
}
