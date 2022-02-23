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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NewSyntax
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.util.CypherExceptionFactory

object Additions {

  // This is functionality that has been added earlier in 4.x and should not work when using CYPHER 3.5
  case object addedFeaturesIn4_x extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.folder.treeExists {

      case u: UseGraph =>
        throw cypherExceptionFactory.syntaxException("The USE clause is not supported in this Cypher version.", u.position)

      // CREATE INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON (n.prop) [OPTIONS {...}]
      case c: CreateBtreeNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Creating index using this syntax is not supported in this Cypher version.", c.position)

      // DROP INDEX name
      case d: DropIndexOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping index by name is not supported in this Cypher version.", d.position)

      // CREATE CONSTRAINT name ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, Some(_), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node key constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, _, IfExistsDoNothing, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT ... IS NODE KEY OPTIONS {...}
      case c@CreateNodeKeyConstraint(_, _, _, _, _, options, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint with options is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, Some(_),_, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named uniqueness constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, _, IfExistsDoNothing, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT ... IS UNIQUE OPTIONS {...}
      case c@CreateUniquePropertyConstraint(_, _, _, _, _, options, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint with options is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, Some(_), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, Some(_), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named relationship existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name
      case d: DropConstraintOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint by name is not supported in this Cypher version.", d.position)

      case e: ExistsSubClause =>
        throw cypherExceptionFactory.syntaxException("Existential subquery is not supported in this Cypher version.", e.position)

      // SHOW [ALL|BTREE|FULLTEXT|LOOKUP] INDEX[ES] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
      case s: ShowIndexesClause =>
        throw cypherExceptionFactory.syntaxException("SHOW INDEXES is not supported in this Cypher version.", s.position)

      // SHOW [ALL|UNIQUE|NODE EXIST[S]|RELATIONSHIP EXIST[S]|EXIST[S]|NODE KEY] CONSTRAINT[S] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
      case s: ShowConstraintsClause =>
        throw cypherExceptionFactory.syntaxException("SHOW CONSTRAINTS is not supported in this Cypher version.", s.position)

      // Administration commands against system database are not supported at all in CYPHER 3.5.
      // This is checked in CompilerFactory, so separate checks for such commands are not needed here.
    }
  }

  // This is functionality that has been added in 4.3 and should not work when using CYPHER 3.5 and CYPHER 4.2
  case object addedFeaturesIn4_3 extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.folder.treeExists {

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS NOT NULL
      case c: CreateNodePropertyExistenceConstraint if !c.oldSyntax =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `IS NOT NULL` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT r.prop IS NOT NULL
      case c: CreateRelationshipPropertyExistenceConstraint if !c.oldSyntax =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `IS NOT NULL` is not supported in this Cypher version.", c.position)

      case c: CreateUser if c.userOptions.homeDatabase.isDefined =>
        throw cypherExceptionFactory.syntaxException("Creating a user with a home database is not supported in this Cypher version.", c.position)

      case c: AlterUser if c.userOptions.homeDatabase.isDefined =>
        throw cypherExceptionFactory.syntaxException("Updating a user with a home database is not supported in this Cypher version.", c.position)

      case c: UnresolvedCall if c.yieldAll =>
        throw cypherExceptionFactory.syntaxException("Procedure call using `YIELD *` is not supported in this Cypher version.", c.position)

      case c: AlterUser if c.ifExists =>
        throw cypherExceptionFactory.syntaxException("Updating a user with `IF EXISTS` is not supported in this Cypher version.", c.position)

      case c: RenameRole =>
        throw cypherExceptionFactory.syntaxException("Changing a role name is not supported in this Cypher version.", c.position)

      case c: RenameUser =>
        throw cypherExceptionFactory.syntaxException("Changing a username is not supported in this Cypher version.", c.position)

      case c: ShowIndexesClause if c.where.isDefined || c.hasYield =>
        throw cypherExceptionFactory.syntaxException("Using YIELD or WHERE when listing indexes is not supported in this Cypher version.", c.position)

      case c: ShowDatabase if c.scope.isInstanceOf[HomeDatabaseScope] =>
        throw cypherExceptionFactory.syntaxException("`SHOW HOME DATABASE` is not supported in this Cypher version.", c.position)

      // GRANT/DENY/REVOKE SET HOME DATABASE ...
      case p@GrantPrivilege(DbmsPrivilege(SetUserHomeDatabaseAction), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("SET USER HOME DATABASE privilege is not supported in this Cypher version.", p.position)
      case p@DenyPrivilege(DbmsPrivilege(SetUserHomeDatabaseAction), _, _, _)   =>
        throw cypherExceptionFactory.syntaxException("SET USER HOME DATABASE privilege is not supported in this Cypher version.", p.position)
      case p@RevokePrivilege(DbmsPrivilege(SetUserHomeDatabaseAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("SET USER HOME DATABASE privilege is not supported in this Cypher version.", p.position)

      // GRANT/DENY/REVOKE ... ON HOME DATABASE ...
      case p@GrantPrivilege(DatabasePrivilege(_, List(HomeDatabaseScope())), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Granting privileges on `HOME DATABASE` is not supported in this Cypher version.", p.position)
      case p@DenyPrivilege(DatabasePrivilege(_, List(HomeDatabaseScope())), _, _, _)      =>
        throw cypherExceptionFactory.syntaxException("Denying privileges on `HOME DATABASE` is not supported in this Cypher version.", p.position)
      case p@RevokePrivilege(DatabasePrivilege(_, List(HomeDatabaseScope())), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Revoking privileges on `HOME DATABASE` is not supported in this Cypher version.", p.position)

      // GRANT/DENY/REVOKE ... ON HOME GRAPH ...
      case p@GrantPrivilege(GraphPrivilege(_, List(HomeGraphScope())), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Granting privileges on `HOME GRAPH` is not supported in this Cypher version.", p.position)
      case p@DenyPrivilege(GraphPrivilege(_, List(HomeGraphScope())), _, _, _)   =>
        throw cypherExceptionFactory.syntaxException("Denying privileges on `HOME GRAPH` is not supported in this Cypher version.", p.position)
      case p@RevokePrivilege(GraphPrivilege(_, List(HomeGraphScope())), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Revoking privileges on `HOME GRAPH` is not supported in this Cypher version.", p.position)

      // CREATE INDEX [name] [IF NOT EXISTS] FOR ()-[n:RelType]-() ON (n.prop) [OPTIONS {...}]
      case c: CreateBtreeRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Relationship property indexes are not supported in this Cypher version.", c.position)

      // SHOW {[PROPERTY] EXISTENCE | PROPERTY EXIST[ENCE]} CONSTRAINTS
      case c@ShowConstraintsClause(_, ExistsConstraints(NewSyntax), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Using `PROPERTY` or `EXISTENCE` when listing property existence constraints is not supported in this Cypher version.", c.position)

      // SHOW {NODE [PROPERTY] EXISTENCE | NODE PROPERTY EXIST[ENCE]} CONSTRAINTS
      case c@ShowConstraintsClause(_, NodeExistsConstraints(NewSyntax), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Using `PROPERTY` or `EXISTENCE` when listing node property existence constraints is not supported in this Cypher version.", c.position)

      // SHOW {RELATIONSHIP [PROPERTY] EXISTENCE | RELATIONSHIP PROPERTY EXIST[ENCE] | REL [PROPERTY] EXIST[ENCE]} CONSTRAINTS
      case c@ShowConstraintsClause(_, RelExistsConstraints(NewSyntax), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Using `REL`, `PROPERTY` or `EXISTENCE` when listing relationship property existence constraints is not supported in this Cypher version.", c.position)

      case c: ShowConstraintsClause if c.where.isDefined || c.hasYield =>
        throw cypherExceptionFactory.syntaxException("Using YIELD or WHERE when listing constraints is not supported in this Cypher version.", c.position)

      case c@ShowIndexesClause(_, FulltextIndexes, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Using `FULLTEXT` when listing indexes is not supported in this Cypher version.", c.position)

      case c@ShowIndexesClause(_, LookupIndexes, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Using `LOOKUP` when listing indexes is not supported in this Cypher version.", c.position)

      // CREATE LOOKUP INDEX ...
      case c: CreateLookupIndex =>
        throw cypherExceptionFactory.syntaxException("Lookup indexes are not supported in this Cypher version.", c.position)

      // CREATE FULLTEXT INDEX ...
      case c: CreateFulltextNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Fulltext indexes can only be created using procedures in this Cypher version.", c.position)
      case c: CreateFulltextRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Fulltext indexes can only be created using procedures in this Cypher version.", c.position)

      // CREATE DATABASE OPTIONS {}
      case c@CreateDatabase(_, _, OptionsMap(_), _) =>
        throw cypherExceptionFactory.syntaxException("Using OPTIONS with CREATE DATABASE is not supported in this Cypher version.", c.position)
      case c@CreateDatabase(_, _, OptionsParam(_), _) =>
        throw cypherExceptionFactory.syntaxException("Using OPTIONS with CREATE DATABASE is not supported in this Cypher version.", c.position)

      // SHOW PROCEDURE[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
      case c: ShowProceduresClause =>
        throw cypherExceptionFactory.syntaxException("`SHOW PROCEDURES` is not supported in this Cypher version.", c.position)

      // SHOW [ALL | BUILT IN | USER DEFINED] FUNCTION[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
      case c: ShowFunctionsClause =>
        throw cypherExceptionFactory.syntaxException("`SHOW FUNCTIONS` is not supported in this Cypher version.", c.position)
    }
  }

}

trait Additions extends {
  def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = {}
}
