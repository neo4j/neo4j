/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;
import java.util.Map;

/**
 * Factory for constructing ASTs.
 * <p>
 * This interface is generic in many dimensions, in order to support type-safe construction of ASTs
 * without depending on the concrete AST type. This architecture allows code which creates/manipulates AST
 * to live independently of the AST, and thus makes sharing and reuse of these components much easier.
 * <p>
 * The factory contains methods for creating AST representing all of Cypher 9, as defined
 * at `https://github.com/opencypher/openCypher/`, and implemented in `https://github.com/opencypher/front-end`.
 * <p>
 * Schema commands like `CREATE/DROP INDEX` as not supported, nor system DSL used in Neo4j.
 *
 * @param <POS> type used to mark the input position of the created AST node.
 */
public interface ASTFactory<STATEMENT,
        QUERY extends STATEMENT,
        CLAUSE,
        RETURN_CLAUSE extends CLAUSE,
        RETURN_ITEM,
        RETURN_ITEMS,
        ORDER_ITEM,
        PATTERN,
        NODE_PATTERN,
        REL_PATTERN,
        PATH_LENGTH,
        SET_CLAUSE extends CLAUSE,
        SET_ITEM,
        REMOVE_ITEM,
        CALL_RESULT_ITEM,
        HINT,
        EXPRESSION,
        PARAMETER extends EXPRESSION,
        VARIABLE extends EXPRESSION,
        PROPERTY extends EXPRESSION,
        MAP_PROJECTION_ITEM,
        USE_GRAPH extends CLAUSE,
        STATEMENT_WITH_GRAPH extends STATEMENT,
        ADMINISTRATION_COMMAND extends STATEMENT_WITH_GRAPH,
        SCHEMA_COMMAND extends STATEMENT_WITH_GRAPH,
        YIELD extends CLAUSE,
        WHERE,
        DATABASE_SCOPE,
        WAIT_CLAUSE,
        ADMINISTRATION_ACTION,
        GRAPH_SCOPE,
        PRIVILEGE_TYPE,
        PRIVILEGE_RESOURCE,
        PRIVILEGE_QUALIFIER,
        SUBQUERY_IN_TRANSACTIONS_PARAMETERS,
        POS>
        extends ASTExpressionFactory<EXPRESSION,PARAMETER,PATTERN,VARIABLE,PROPERTY,MAP_PROJECTION_ITEM,POS>
{
    final class NULL
    {
        private NULL()
        {
            throw new IllegalStateException( "This class should not be instantiated, use `null` instead." );
        }
    }

    class StringPos<POS>
    {
        public final String string;
        public final POS pos;

        public StringPos( String string, POS pos )
        {
            this.string = string;
            this.pos = pos;
        }
    }

    // QUERY

    QUERY newSingleQuery( POS p, List<CLAUSE> clauses );

    QUERY newSingleQuery( List<CLAUSE> clauses );

    QUERY newUnion( POS p, QUERY lhs, QUERY rhs, boolean all );

    QUERY periodicCommitQuery( POS p, POS periodicCommitPosition, String batchSize, CLAUSE loadCsv, List<CLAUSE> queryBody );

    USE_GRAPH useClause( POS p, EXPRESSION e );

    RETURN_CLAUSE newReturnClause( POS p, boolean distinct,
                                   RETURN_ITEMS returnItems,
                                   List<ORDER_ITEM> order,
                                   POS orderPos,
                                   EXPRESSION skip,
                                   POS skipPosition,
                                   EXPRESSION limit,
                                   POS limitPosition );

    RETURN_ITEMS newReturnItems( POS p, boolean returnAll, List<RETURN_ITEM> returnItems );

    RETURN_ITEM newReturnItem( POS p, EXPRESSION e, VARIABLE v );

    RETURN_ITEM newReturnItem( POS p, EXPRESSION e, int eStartOffset, int eEndOffset );

    ORDER_ITEM orderDesc( POS p, EXPRESSION e );

    ORDER_ITEM orderAsc( POS p, EXPRESSION e );

    WHERE whereClause( POS p, EXPRESSION e );

    CLAUSE withClause( POS p, RETURN_CLAUSE returnClause, WHERE where );

    CLAUSE matchClause( POS p, boolean optional, List<PATTERN> patterns, POS patternPos, List<HINT> hints, WHERE where );

    HINT usingIndexHint( POS p, VARIABLE v, String labelOrRelType, List<String> properties, boolean seekOnly );

    HINT usingJoin( POS p, List<VARIABLE> joinVariables );

    HINT usingScan( POS p, VARIABLE v, String labelOrRelType );

    CLAUSE createClause( POS p, List<PATTERN> patterns );

    SET_CLAUSE setClause( POS p, List<SET_ITEM> setItems );

    SET_ITEM setProperty( PROPERTY property, EXPRESSION value );

    SET_ITEM setVariable( VARIABLE variable, EXPRESSION value );

    SET_ITEM addAndSetVariable( VARIABLE variable, EXPRESSION value );

    SET_ITEM setLabels( VARIABLE variable, List<StringPos<POS>> value );

    CLAUSE removeClause( POS p, List<REMOVE_ITEM> removeItems );

    REMOVE_ITEM removeProperty( PROPERTY property );

    REMOVE_ITEM removeLabels( VARIABLE variable, List<StringPos<POS>> labels );

    CLAUSE deleteClause( POS p, boolean detach, List<EXPRESSION> expressions );

    CLAUSE unwindClause( POS p, EXPRESSION e, VARIABLE v );

    enum MergeActionType
    {
        OnCreate,
        OnMatch
    }

    CLAUSE  mergeClause( POS p, PATTERN pattern, List<SET_CLAUSE> setClauses, List<MergeActionType> actionTypes, List<POS> positions );

    CLAUSE callClause( POS p,
                       POS namespacePosition,
                       POS procedureNamePosition,
                       POS procedureResultPosition,
                       List<String> namespace,
                       String name,
                       List<EXPRESSION> arguments,
                       boolean yieldAll,
                       List<CALL_RESULT_ITEM> resultItems,
                       WHERE where );

    CALL_RESULT_ITEM callResultItem( POS p, String name, VARIABLE v );

    PATTERN namedPattern( VARIABLE v, PATTERN pattern );

    PATTERN shortestPathPattern( POS p, PATTERN pattern );

    PATTERN allShortestPathsPattern( POS p, PATTERN pattern );

    PATTERN everyPathPattern( List<NODE_PATTERN> nodes, List<REL_PATTERN> relationships );

    NODE_PATTERN nodePattern( POS p, VARIABLE v, List<StringPos<POS>> labels, EXPRESSION properties, EXPRESSION predicate );

    REL_PATTERN relationshipPattern( POS p,
                                     boolean left,
                                     boolean right,
                                     VARIABLE v,
                                     List<StringPos<POS>> relTypes,
                                     PATH_LENGTH pathLength,
                                     EXPRESSION properties,
                                     boolean legacyTypeSeparator );

    /**
     * Create a path-length object used to specify path lengths for variable length patterns.
     *
     * Note that paths will be reported in a quite specific manner:
     *     Cypher       minLength   maxLength
     *     ----------------------------------
     *     [*]          null        null
     *     [*2]         "2"         "2"
     *     [*2..]       "2"         ""
     *     [*..3]       ""          "3"
     *     [*2..3]      "2"         "3"
     *     [*..]        ""          ""      <- separate from [*] to allow specific error messages
     */
    PATH_LENGTH pathLength( POS p, POS pMin, POS pMax, String minLength, String maxLength );

    CLAUSE loadCsvClause( POS p, boolean headers, EXPRESSION source, VARIABLE v, String fieldTerminator );

    CLAUSE foreachClause( POS p, VARIABLE v, EXPRESSION list, List<CLAUSE> clauses );

    CLAUSE subqueryClause( POS p, QUERY subquery, SUBQUERY_IN_TRANSACTIONS_PARAMETERS inTransactions );

    SUBQUERY_IN_TRANSACTIONS_PARAMETERS subqueryInTransactionsParams( POS p, EXPRESSION batchSize );

    // Commands
    STATEMENT_WITH_GRAPH useGraph( STATEMENT_WITH_GRAPH statement, USE_GRAPH useGraph );

    ADMINISTRATION_COMMAND hasCatalog( STATEMENT statement );

    // Show Command Clauses

    YIELD yieldClause( POS p,
                       boolean returnAll,
                       List<RETURN_ITEM> returnItems,
                       POS returnItemsPosition,
                       List<ORDER_ITEM> orderBy,
                       POS orderPos,
                       EXPRESSION skip,
                       POS skipPosition,
                       EXPRESSION limit,
                       POS limitPosition,
                       WHERE where );

    CLAUSE showIndexClause( POS p, ShowCommandFilterTypes indexType, boolean brief, boolean verbose, WHERE where, boolean hasYield );

    CLAUSE showConstraintClause( POS p, ShowCommandFilterTypes constraintType, boolean brief, boolean verbose, WHERE where, boolean hasYield );

    CLAUSE showProcedureClause( POS p, boolean currentUser, String user, WHERE where, boolean hasYield );

    CLAUSE showFunctionClause( POS p, ShowCommandFilterTypes functionType, boolean currentUser, String user, WHERE where, boolean hasYield );

    // Schema Commands
    // Constraint Commands

    SCHEMA_COMMAND createConstraint( POS p, ConstraintType constraintType, boolean replace, boolean ifNotExists, String constraintName, VARIABLE variable,
                                     StringPos<POS> label, List<PROPERTY> properties, SimpleEither<Map<String, EXPRESSION>, PARAMETER> options,
                                     boolean containsOn, ConstraintVersion constraintVersion );

    SCHEMA_COMMAND dropConstraint( POS p, String name, boolean ifExists );

    SCHEMA_COMMAND dropConstraint( POS p, ConstraintType constraintType, VARIABLE variable, StringPos<POS> label, List<PROPERTY> properties );

    // Index Commands

    SCHEMA_COMMAND createIndexWithOldSyntax( POS p, StringPos<POS> label, List<StringPos<POS>> properties );

    SCHEMA_COMMAND createLookupIndex( POS p, boolean replace, boolean ifNotExists, boolean isNode, String indexName, VARIABLE variable,
                                      StringPos<POS> functionName, VARIABLE functionParameter, SimpleEither<Map<String,EXPRESSION>,PARAMETER> options );

    SCHEMA_COMMAND createIndex( POS p, boolean replace, boolean ifNotExists, boolean isNode, String indexName, VARIABLE variable, StringPos<POS> label,
                                List<PROPERTY> properties, SimpleEither<Map<String,EXPRESSION>,PARAMETER> options, CreateIndexTypes indexType );

    SCHEMA_COMMAND createFulltextIndex( POS p, boolean replace, boolean ifNotExists, boolean isNode, String indexName, VARIABLE variable,
                                        List<StringPos<POS>> labels, List<PROPERTY> properties, SimpleEither<Map<String,EXPRESSION>,PARAMETER> options );

    SCHEMA_COMMAND dropIndex( POS p, String name, boolean ifExists );

    SCHEMA_COMMAND dropIndex( POS p, StringPos<POS> label, List<StringPos<POS>> propertyNames );

    // Administration Commands
    // Role Administration Commands

    ADMINISTRATION_COMMAND createRole( POS p,
                                       boolean replace,
                                       SimpleEither<String, PARAMETER> roleName,
                                       SimpleEither<String, PARAMETER> fromRole,
                                       boolean ifNotExists );

    ADMINISTRATION_COMMAND dropRole( POS p, SimpleEither<String, PARAMETER> roleName, boolean ifExists );

    ADMINISTRATION_COMMAND renameRole( POS p, SimpleEither<String, PARAMETER> fromRoleName, SimpleEither<String, PARAMETER> toRoleName, boolean ifExists );

    ADMINISTRATION_COMMAND showRoles( POS p, boolean withUsers, boolean showAll, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where );

    ADMINISTRATION_COMMAND grantRoles( POS p, List<SimpleEither<String,PARAMETER>> roles, List<SimpleEither<String,PARAMETER>> users );

    ADMINISTRATION_COMMAND revokeRoles( POS p, List<SimpleEither<String,PARAMETER>> roles, List<SimpleEither<String,PARAMETER>> users );

    // User Administration Commands

    ADMINISTRATION_COMMAND createUser( POS p,
                                       boolean replace,
                                       boolean ifNotExists,
                                       SimpleEither<String, PARAMETER> username,
                                       EXPRESSION password,
                                       boolean encrypted,
                                       boolean changeRequired,
                                       Boolean suspended,
                                       SimpleEither<String, PARAMETER> homeDatabase );

    ADMINISTRATION_COMMAND dropUser( POS p, boolean ifExists, SimpleEither<String, PARAMETER> username );

    ADMINISTRATION_COMMAND renameUser( POS p, SimpleEither<String, PARAMETER> fromUserName, SimpleEither<String, PARAMETER> toUserName, boolean ifExists );

    ADMINISTRATION_COMMAND setOwnPassword( POS p, EXPRESSION currentPassword, EXPRESSION newPassword );

    ADMINISTRATION_COMMAND alterUser( POS p,
                                      boolean ifExists,
                                      SimpleEither<String,PARAMETER> username,
                                      EXPRESSION password,
                                      boolean encrypted,
                                      Boolean changeRequired,
                                      Boolean suspended,
                                      SimpleEither<String,PARAMETER> homeDatabase,
                                      boolean removeHome );

    EXPRESSION passwordExpression( PARAMETER password );

    EXPRESSION passwordExpression( POS p, String password );

    ADMINISTRATION_COMMAND showUsers( POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where );

    ADMINISTRATION_COMMAND showCurrentUser( POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where );

    // Privilege Commands

    ADMINISTRATION_COMMAND grantPrivilege( POS p, List<SimpleEither<String,PARAMETER>> roles, PRIVILEGE_TYPE privilege );

    ADMINISTRATION_COMMAND denyPrivilege( POS p, List<SimpleEither<String,PARAMETER>> roles, PRIVILEGE_TYPE privilege );

    ADMINISTRATION_COMMAND revokePrivilege( POS p, List<SimpleEither<String,PARAMETER>> roles, PRIVILEGE_TYPE privilege, boolean revokeGrant,
                                            boolean revokeDeny );

    PRIVILEGE_TYPE databasePrivilege( POS p, ADMINISTRATION_ACTION action, List<DATABASE_SCOPE> scope, List<PRIVILEGE_QUALIFIER> qualifier );

    PRIVILEGE_TYPE dbmsPrivilege( POS p, ADMINISTRATION_ACTION action, List<PRIVILEGE_QUALIFIER> qualifier );

    PRIVILEGE_TYPE graphPrivilege( POS p,
                                   ADMINISTRATION_ACTION action,
                                   List<GRAPH_SCOPE> scope,
                                   PRIVILEGE_RESOURCE resource,
                                   List<PRIVILEGE_QUALIFIER> qualifier );

    ADMINISTRATION_ACTION privilegeAction( ActionType action );

    // Resources

    PRIVILEGE_RESOURCE propertiesResource( POS p, List<String> property );

    PRIVILEGE_RESOURCE allPropertiesResource( POS p );

    PRIVILEGE_RESOURCE labelsResource( POS p, List<String> label );

    PRIVILEGE_RESOURCE allLabelsResource( POS p );

    PRIVILEGE_RESOURCE databaseResource( POS p );

    PRIVILEGE_RESOURCE noResource( POS p );

    PRIVILEGE_QUALIFIER labelQualifier( POS p, String label );

    PRIVILEGE_QUALIFIER relationshipQualifier( POS p, String relationshipType );

    PRIVILEGE_QUALIFIER elementQualifier( POS p, String name );

    PRIVILEGE_QUALIFIER allElementsQualifier( POS p );

    PRIVILEGE_QUALIFIER allLabelsQualifier( POS p );

    PRIVILEGE_QUALIFIER allRelationshipsQualifier( POS p );

    List<PRIVILEGE_QUALIFIER> allQualifier();

    List<PRIVILEGE_QUALIFIER> allDatabasesQualifier();

    List<PRIVILEGE_QUALIFIER> userQualifier( List<SimpleEither<String,PARAMETER>> users );

    List<PRIVILEGE_QUALIFIER> allUsersQualifier();

    List<GRAPH_SCOPE> graphScopes( POS p, List<SimpleEither<String,PARAMETER>> graphNames, ScopeType scopeType );

    List<DATABASE_SCOPE> databaseScopes( POS p, List<SimpleEither<String,PARAMETER>> databaseNames, ScopeType scopeType );

    // Database Administration Commands

    ADMINISTRATION_COMMAND createDatabase( POS p,
                                           boolean replace,
                                           SimpleEither<String,PARAMETER> databaseName,
                                           boolean ifNotExists,
                                           WAIT_CLAUSE waitClause,
                                           SimpleEither<Map<String,EXPRESSION>,PARAMETER> options );

    ADMINISTRATION_COMMAND dropDatabase( POS p, SimpleEither<String,PARAMETER> databaseName, boolean ifExists, boolean dumpData, WAIT_CLAUSE wait );

    ADMINISTRATION_COMMAND alterDatabase( POS p, SimpleEither<String,PARAMETER> databaseName, boolean ifExists, AccessType accessType );

    ADMINISTRATION_COMMAND showDatabase( POS p, DATABASE_SCOPE scope, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where );

    ADMINISTRATION_COMMAND startDatabase( POS p, SimpleEither<String,PARAMETER> databaseName, WAIT_CLAUSE wait );

    ADMINISTRATION_COMMAND stopDatabase( POS p, SimpleEither<String,PARAMETER> databaseName, WAIT_CLAUSE wait );

    DATABASE_SCOPE databaseScope( POS p, SimpleEither<String,PARAMETER> databaseName, boolean isDefault, boolean isHome );

    WAIT_CLAUSE wait( boolean wait, long seconds );
}
