/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.literal.interpreter;

import java.time.Clock;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory.NULL;
import org.neo4j.cypher.internal.parser.common.ast.factory.AccessType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ActionType;
import org.neo4j.cypher.internal.parser.common.ast.factory.CallInTxsOnErrorBehaviourType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType;
import org.neo4j.cypher.internal.parser.common.ast.factory.CreateIndexTypes;
import org.neo4j.cypher.internal.parser.common.ast.factory.HintIndexType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ParameterType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ParserCypherTypeName;
import org.neo4j.cypher.internal.parser.common.ast.factory.ParserNormalForm;
import org.neo4j.cypher.internal.parser.common.ast.factory.ParserTrimSpecification;
import org.neo4j.cypher.internal.parser.common.ast.factory.ScopeType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ShowCommandFilterTypes;
import org.neo4j.cypher.internal.parser.common.ast.factory.SimpleEither;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Interprets literal AST nodes and output a corresponding java object.
 */
public class LiteralInterpreter
        implements ASTFactory<
                NULL, // STATEMENTS,
                NULL, // STATEMENT,
                NULL, // QUERY extends STATEMENT,
                NULL, // CLAUSE,
                NULL, // FINISH_CLAUSE extends CLAUSE,
                NULL, // RETURN_CLAUSE extends CLAUSE,
                NULL, // RETURN_ITEM,
                NULL, // RETURN_ITEMS,
                NULL, // ORDER_ITEM,
                NULL, // PATTERN,
                NULL, // NODE_PATTERN extends PATTERN_ATOM,
                NULL, // REL_PATTERN extends PATTERN_ATOM,
                NULL, // PATH_LENGTH,
                NULL, // SET_CLAUSE extends CLAUSE,
                NULL, // SET_ITEM,
                NULL, // REMOVE_ITEM,
                NULL, // CALL_RESULT_ITEM,
                NULL, // HINT,
                Object, // EXPRESSION,
                NULL, // LABEL_EXPRESSION,
                Object, // FUNCTION_INVOCATION extends EXPRESSION,
                Object, // PARAMETER extends EXPRESSION,
                Object, // VARIABLE extends EXPRESSION,
                Object, // PROPERTY extends EXPRESSION,
                NULL, // MAP_PROJECTION_ITEM,
                NULL, // USE_GRAPH extends CLAUSE,
                NULL, // STATEMENT_WITH_GRAPH extends STATEMENT,
                NULL, // ADMINISTRATION_COMMAND extends STATEMENT_WITH_GRAPH,
                NULL, // SCHEMA_COMMAND extends STATEMENT_WITH_GRAPH,
                NULL, // YIELD extends CLAUSE,
                NULL, // WHERE,
                NULL, // DATABASE_SCOPE,
                NULL, // WAIT_CLAUSE,
                NULL, // ADMINISTRATION_ACTION,
                NULL, // GRAPH_SCOPE,
                NULL, // PRIVILEGE_TYPE,
                NULL, // PRIVILEGE_RESOURCE,
                NULL, // PRIVILEGE_QUALIFIER,
                NULL, // AUTH,
                NULL, // AUTH_ATTRIBUTE,
                NULL, // SUBQUERY_IN_TRANSACTIONS_BATCH_PARAMETERS,
                NULL, // SUBQUERY_IN_TRANSACTIONS_CONCURRENCY_PARAMETERS,
                NULL, // SUBQUERY_IN_TRANSACTIONS_ERROR_PARAMETERS,
                NULL, // SUBQUERY_IN_TRANSACTIONS_REPORT_PARAMETERS,
                NULL, // SUBQUERY_IN_TRANSACTIONS_PARAMETERS,
                NULL, // POS,
                NULL, // ENTITY_TYPE,
                NULL, // PATH_PATTERN_LENGTH,
                NULL, // PATTERN_ATOM
                NULL, // DATABASE NAME
                NULL, // PATTERN_SELECTOR
                NULL, // MATCH_MODE
                NULL // PATTERN_ELEMENT
        > {

    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    public static final String LONG_MIN_VALUE_DECIMAL_STRING =
            Long.toString(Long.MIN_VALUE).substring(1);
    public static final String LONG_MIN_VALUE_HEXADECIMAL_STRING =
            "0x" + Long.toString(Long.MIN_VALUE, 16).substring(1);
    public static final String LONG_MIN_VALUE_OCTAL_STRING_OLD_SYNTAX =
            "0" + Long.toString(Long.MIN_VALUE, 8).substring(1);
    public static final String LONG_MIN_VALUE_OCTAL_STRING =
            "0o" + Long.toString(Long.MIN_VALUE, 8).substring(1);

    @Override
    public NULL statements(List<NULL> nulls) {
        throw new UnsupportedOperationException("statements is not a literal");
    }

    @Override
    public NULL newSingleQuery(NULL p, List<NULL> nulls) {
        throw new UnsupportedOperationException("newSingleQuery is not a literal");
    }

    @Override
    public NULL newSingleQuery(List<NULL> nulls) {
        throw new UnsupportedOperationException("newSingleQuery is not a literal");
    }

    @Override
    public NULL newUnion(NULL p, NULL lhs, NULL rhs, boolean all) {
        throw new UnsupportedOperationException("newUnion is not a literal");
    }

    @Override
    public NULL directUseClause(NULL p, NULL o) {
        throw new UnsupportedOperationException("directUseClause is not a literal");
    }

    @Override
    public NULL functionUseClause(NULL p, Object function) {
        throw new UnsupportedOperationException("directUseClause is not a literal");
    }

    @Override
    public NULL newFinishClause(NULL p) {
        throw new UnsupportedOperationException("newFinishClause is not a literal");
    }

    @Override
    public NULL newReturnClause(
            NULL p,
            boolean distinct,
            NULL nulls,
            List<NULL> order,
            NULL orderPos,
            Object skip,
            NULL skipPosition,
            Object limit,
            NULL limitPosition) {
        throw new UnsupportedOperationException("newReturnClause is not a literal");
    }

    @Override
    public NULL newReturnItems(NULL p, boolean returnAll, List<NULL> returnItems) {
        throw new UnsupportedOperationException("newReturnItems is not a literal");
    }

    @Override
    public NULL newReturnItem(NULL p, Object e, Object v) {
        throw new UnsupportedOperationException("newReturnItem is not a literal");
    }

    @Override
    public NULL newReturnItem(NULL p, Object e, int eStartOffset, int eEndOffset) {
        throw new UnsupportedOperationException("newReturnItem is not a literal");
    }

    @Override
    public NULL orderDesc(NULL p, Object e) {
        throw new UnsupportedOperationException("orderDesc is not a literal");
    }

    @Override
    public NULL orderAsc(NULL p, Object e) {
        throw new UnsupportedOperationException("orderAsc is not a literal");
    }

    @Override
    public NULL whereClause(NULL p, Object where) {
        throw new UnsupportedOperationException("withClause is not a literal");
    }

    @Override
    public NULL withClause(NULL p, NULL aNull, NULL where) {
        throw new UnsupportedOperationException("withClause is not a literal");
    }

    @Override
    public NULL matchClause(
            NULL p,
            boolean optional,
            NULL matchMode,
            List<NULL> nulls,
            NULL patternPos,
            List<NULL> nulls2,
            NULL where) {
        throw new UnsupportedOperationException("matchClause is not a literal");
    }

    @Override
    public NULL usingIndexHint(
            NULL p,
            Object v,
            String labelOrRelType,
            List<String> properties,
            boolean seekOnly,
            HintIndexType indexType) {
        throw new UnsupportedOperationException("usingIndexHint is not a literal");
    }

    @Override
    public NULL usingJoin(NULL p, List<Object> joinVariables) {
        throw new UnsupportedOperationException("usingJoin is not a literal");
    }

    @Override
    public NULL usingScan(NULL p, Object v, String labelOrRelType) {
        throw new UnsupportedOperationException("usingScan is not a literal");
    }

    @Override
    public NULL createClause(NULL p, List<NULL> nulls) {
        throw new UnsupportedOperationException("createClause is not a literal");
    }

    @Override
    public NULL insertClause(NULL p, List<NULL> nulls) {
        throw new UnsupportedOperationException("insertClause is not a literal");
    }

    @Override
    public NULL setClause(NULL p, List<NULL> nulls) {
        throw new UnsupportedOperationException("setClause is not a literal");
    }

    @Override
    public NULL setProperty(Object o, Object value) {
        throw new UnsupportedOperationException("setProperty is not a literal");
    }

    @Override
    public NULL setDynamicProperty(Object o, Object value) {
        throw new UnsupportedOperationException("setDynamicProperty is not a literal");
    }

    @Override
    public NULL setVariable(Object o, Object value) {
        throw new UnsupportedOperationException("setVariable is not a literal");
    }

    @Override
    public NULL addAndSetVariable(Object o, Object value) {
        throw new UnsupportedOperationException("addAndSetVariable is not a literal");
    }

    @Override
    public NULL setLabels(Object o, List<StringPos<NULL>> labels, List<Object> dynamicLabels, boolean containsIs) {
        throw new UnsupportedOperationException("setLabels is not a literal");
    }

    @Override
    public NULL removeClause(NULL p, List<NULL> nulls) {
        throw new UnsupportedOperationException("removeClause is not a literal");
    }

    @Override
    public NULL removeProperty(Object o) {
        throw new UnsupportedOperationException("removeProperty is not a literal");
    }

    @Override
    public NULL removeDynamicProperty(Object o) {
        throw new UnsupportedOperationException("removeDynamicProperty is not a literal");
    }

    @Override
    public NULL removeLabels(Object o, List<StringPos<NULL>> labels, List<Object> dynamicLabels, boolean containsIs) {
        throw new UnsupportedOperationException("removeLabels is not a literal");
    }

    @Override
    public NULL deleteClause(NULL p, boolean detach, List<Object> objects) {
        throw new UnsupportedOperationException("deleteClause is not a literal");
    }

    @Override
    public NULL unwindClause(NULL p, Object e, Object v) {
        throw new UnsupportedOperationException("unwindClause is not a literal");
    }

    @Override
    public NULL mergeClause(
            NULL p, NULL aNull, List<NULL> setClauses, List<MergeActionType> actionTypes, List<NULL> positions) {
        throw new UnsupportedOperationException("mergeClause is not a literal");
    }

    @Override
    public NULL callClause(
            NULL p,
            NULL procedureResultPosition,
            NULL procedureNamePosition,
            NULL namespacePosition,
            List<String> namespace,
            String name,
            List<Object> arguments,
            boolean yieldAll,
            List<NULL> nulls,
            NULL where) {
        throw new UnsupportedOperationException("callClause is not a literal");
    }

    @Override
    public NULL callResultItem(NULL p, String name, Object v) {
        throw new UnsupportedOperationException("callResultItem is not a literal");
    }

    @Override
    public NULL patternWithSelector(NULL selector, NULL patternPart) {
        throw new UnsupportedOperationException("patternWithSelector is not a literal");
    }

    @Override
    public NULL namedPattern(Object v, NULL aNull) {
        throw new UnsupportedOperationException("namedPattern is not a literal");
    }

    @Override
    public NULL shortestPathPattern(NULL p, NULL aNull) {
        throw new UnsupportedOperationException("shortestPathPattern is not a literal");
    }

    @Override
    public NULL allShortestPathsPattern(NULL p, NULL aNull) {
        throw new UnsupportedOperationException("allShortestPathsPattern is not a literal");
    }

    @Override
    public NULL pathPattern(NULL aNull) {
        throw new UnsupportedOperationException("pathPattern is not a literal");
    }

    @Override
    public NULL insertPathPattern(List<NULL> nulls) {
        throw new UnsupportedOperationException("insertPathPattern is not a literal");
    }

    @Override
    public NULL patternElement(List<NULL> nulls) {
        throw new UnsupportedOperationException("patternElement is not a literal");
    }

    @Override
    public NULL anyPathSelector(String count, NULL pCount, NULL p) {
        throw new UnsupportedOperationException("anyPathSelector is not a literal");
    }

    @Override
    public NULL allPathSelector(NULL p) {
        throw new UnsupportedOperationException("allPathSelector is not a literal");
    }

    @Override
    public NULL anyShortestPathSelector(String count, NULL pCount, NULL p) {
        throw new UnsupportedOperationException("anyShortestPathSelector is not a literal");
    }

    @Override
    public NULL allShortestPathSelector(NULL p) {
        throw new UnsupportedOperationException("allShortestPathSelector is not a literal");
    }

    @Override
    public NULL shortestGroupsSelector(String count, NULL countPosition, NULL position) {
        throw new UnsupportedOperationException("shortestGroupsSelector is not a literal");
    }

    @Override
    public NULL nodePattern(NULL p, Object v, NULL aNull, Object properties, Object predicate) {
        throw new UnsupportedOperationException("nodePattern is not a literal");
    }

    @Override
    public NULL relationshipPattern(
            NULL p,
            boolean left,
            boolean right,
            Object v,
            NULL labelExpression,
            NULL aNull,
            Object properties,
            Object predicate) {
        throw new UnsupportedOperationException("relationshipPattern is not a literal");
    }

    @Override
    public NULL pathLength(NULL p, NULL pMin, NULL mMax, String minLength, String maxLength) {
        throw new UnsupportedOperationException("pathLength is not a literal");
    }

    @Override
    public NULL intervalPathQuantifier(
            NULL p, NULL posLowerBound, NULL posUpperBound, String lowerBound, String upperBound) {
        throw new UnsupportedOperationException("intervalPathQuantifier is not a literal");
    }

    @Override
    public NULL fixedPathQuantifier(NULL p, NULL valuePos, String value) {
        throw new UnsupportedOperationException("fixedPathQuantifier is not a literal");
    }

    @Override
    public NULL plusPathQuantifier(NULL p) {
        throw new UnsupportedOperationException("plusPathQuantifier is not a literal");
    }

    @Override
    public NULL starPathQuantifier(NULL p) {
        throw new UnsupportedOperationException("starPathQuantifier is not a literal");
    }

    @Override
    public NULL repeatableElements(NULL p) {
        throw new UnsupportedOperationException("repeatableElements is not a literal");
    }

    @Override
    public NULL differentRelationships(NULL p) {
        throw new UnsupportedOperationException("differentRelationships is not a literal");
    }

    @Override
    public NULL parenthesizedPathPattern(NULL p, NULL internalPattern, Object where, NULL aNull) {
        throw new UnsupportedOperationException("parenthesizedPathPattern is not a literal");
    }

    @Override
    public NULL quantifiedRelationship(NULL rel, NULL quantifier) {
        throw new UnsupportedOperationException("quantifiedRelationship is not a literal");
    }

    @Override
    public NULL loadCsvClause(NULL p, boolean headers, Object source, Object v, String fieldTerminator) {
        throw new UnsupportedOperationException("loadCsvClause is not a literal");
    }

    @Override
    public NULL foreachClause(NULL p, Object v, Object list, List<NULL> nulls) {
        throw new UnsupportedOperationException("foreachClause is not a literal");
    }

    @Override
    public NULL subqueryClause(
            NULL p, NULL subquery, NULL inTransactions, boolean scopeAll, boolean hasScope, List<Object> variables) {
        throw new UnsupportedOperationException("subqueryClause is not a literal");
    }

    @Override
    public NULL subqueryInTransactionsParams(
            NULL p, NULL batchParams, NULL concurrencyParams, NULL errorParams, NULL reportParams) {
        throw new UnsupportedOperationException("subqueryInTransactionsParams is not a literal");
    }

    @Override
    public NULL subqueryInTransactionsBatchParameters(NULL p, Object batchSize) {
        throw new UnsupportedOperationException("subqueryInTransactionsBatchParameters is not a literal");
    }

    @Override
    public NULL subqueryInTransactionsConcurrencyParameters(NULL p, Object concurrency) {
        throw new UnsupportedOperationException("subqueryInTransactionsConcurrencyParameters is not a literal");
    }

    @Override
    public NULL subqueryInTransactionsErrorParameters(NULL p, CallInTxsOnErrorBehaviourType onErrorBehaviour) {
        throw new UnsupportedOperationException("subqueryInTransactionsErrorParameters is not a literal");
    }

    @Override
    public NULL subqueryInTransactionsReportParameters(NULL p, Object v) {
        throw new UnsupportedOperationException("subqueryInTransactionsReportParameters is not a literal");
    }

    @Override
    public NULL orderBySkipLimitClause(
            NULL t, List<NULL> order, NULL orderPos, Object skip, NULL skipPos, Object limit, NULL limitPos) {
        throw new UnsupportedOperationException("orderBySkipLimitClause is not a literal");
    }

    @Override
    public NULL yieldClause(
            NULL p,
            boolean returnAll,
            List<NULL> nulls,
            NULL returnItemsPosition,
            List<NULL> orderBy,
            NULL orderPos,
            Object skip,
            NULL skipPosition,
            Object limit,
            NULL limitPosition,
            NULL where) {
        throw new UnsupportedOperationException("yieldClause is not a literal");
    }

    @Override
    public NULL showIndexClause(NULL p, ShowCommandFilterTypes indexType, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("showIndexClause is not a literal");
    }

    @Override
    public NULL showConstraintClause(NULL p, ShowCommandFilterTypes constraintType, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("showConstraintClause is not a literal");
    }

    @Override
    public NULL showProcedureClause(NULL p, boolean currentUser, String user, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("showProcedureClause is not a literal");
    }

    @Override
    public NULL showFunctionClause(
            NULL p,
            ShowCommandFilterTypes functionType,
            boolean currentUser,
            String user,
            NULL where,
            NULL yieldClause) {
        throw new UnsupportedOperationException("showFunctionClause is not a literal");
    }

    @Override
    public NULL showTransactionsClause(NULL p, SimpleEither<List<String>, Object> ids, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("showTransactionsClause is not a literal");
    }

    @Override
    public NULL terminateTransactionsClause(
            NULL p, SimpleEither<List<String>, Object> ids, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("terminateTransactionsClause is not a literal");
    }

    @Override
    public NULL showSettingsClause(NULL p, SimpleEither<List<String>, Object> ids, NULL where, NULL yieldClause) {
        throw new UnsupportedOperationException("showSettingsClause is not a literal");
    }

    @Override
    public NULL turnYieldToWith(NULL yieldClause) {
        throw new UnsupportedOperationException("turnYieldToWith is not a literal");
    }

    // Schema commands

    @Override
    public NULL createConstraint(
            NULL p,
            ConstraintType constraintType,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<NULL>, Object> name,
            Object o,
            StringPos<NULL> label,
            List<Object> objects,
            ParserCypherTypeName propertyType,
            SimpleEither<Map<String, Object>, Object> options) {
        throw new UnsupportedOperationException("createConstraint is not a literal");
    }

    @Override
    public NULL dropConstraint(NULL p, SimpleEither<StringPos<NULL>, Object> name, boolean ifExists) {
        throw new UnsupportedOperationException("dropConstraint is not a literal");
    }

    @Override
    public NULL createLookupIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, Object> indexName,
            Object o,
            StringPos<NULL> functionName,
            Object functionParameter,
            SimpleEither<Map<String, Object>, Object> options) {
        throw new UnsupportedOperationException("createLookupIndex is not a literal");
    }

    @Override
    public NULL createIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, Object> indexName,
            Object o,
            StringPos<NULL> label,
            List<Object> objects,
            SimpleEither<Map<String, Object>, Object> options,
            CreateIndexTypes indexType) {
        throw new UnsupportedOperationException("createIndex is not a literal");
    }

    @Override
    public NULL createFulltextIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, Object> indexName,
            Object o,
            List<StringPos<NULL>> labels,
            List<Object> objects,
            SimpleEither<Map<String, Object>, Object> options) {
        throw new UnsupportedOperationException("createFulltextIndex is not a literal");
    }

    @Override
    public NULL dropIndex(NULL p, SimpleEither<StringPos<NULL>, Object> name, boolean ifExists) {
        throw new UnsupportedOperationException("dropIndex is not a literal");
    }

    // Administration Commands

    @Override
    public NULL useGraph(NULL aNull, NULL aNull2) {
        throw new UnsupportedOperationException("useGraph is not a literal");
    }

    // Role commands

    @Override
    public NULL createRole(
            NULL p,
            boolean replace,
            SimpleEither<StringPos<NULL>, Object> roleName,
            SimpleEither<StringPos<NULL>, Object> from,
            boolean ifNotExists) {
        throw new UnsupportedOperationException("createRole is not a literal");
    }

    @Override
    public NULL dropRole(NULL p, SimpleEither<StringPos<NULL>, Object> roleName, boolean ifExists) {
        throw new UnsupportedOperationException("dropRole is not a literal");
    }

    @Override
    public NULL renameRole(
            NULL p,
            SimpleEither<StringPos<NULL>, Object> fromRoleName,
            SimpleEither<StringPos<NULL>, Object> toRoleName,
            boolean ifExists) {
        throw new UnsupportedOperationException("renameRole is not a literal");
    }

    @Override
    public NULL showRoles(
            NULL p, boolean withUsers, boolean showAll, NULL yieldExpr, NULL returnWithoutGraph, NULL where) {
        throw new UnsupportedOperationException("showRoles is not a literal");
    }

    @Override
    public NULL grantRoles(
            NULL p,
            List<SimpleEither<StringPos<NULL>, Object>> roles,
            List<SimpleEither<StringPos<NULL>, Object>> users) {
        throw new UnsupportedOperationException("grantRoles is not a literal");
    }

    @Override
    public NULL revokeRoles(
            NULL p,
            List<SimpleEither<StringPos<NULL>, Object>> roles,
            List<SimpleEither<StringPos<NULL>, Object>> users) {
        throw new UnsupportedOperationException("revokeRoles is not a literal");
    }

    // User commands

    @Override
    public NULL createUser(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<NULL>, Object> username,
            Boolean suspended,
            NULL homeDatabase,
            List<NULL> auths,
            List<NULL> systemAuthAttributes) {
        throw new UnsupportedOperationException("createUser is not a literal");
    }

    @Override
    public NULL dropUser(NULL p, boolean ifExists, SimpleEither<StringPos<NULL>, Object> username) {
        throw new UnsupportedOperationException("dropUser is not a literal");
    }

    @Override
    public NULL renameUser(
            NULL p,
            SimpleEither<StringPos<NULL>, Object> fromUserName,
            SimpleEither<StringPos<NULL>, Object> toUserName,
            boolean ifExists) {
        throw new UnsupportedOperationException("renameUser is not a literal");
    }

    @Override
    public NULL setOwnPassword(NULL p, Object currentPassword, Object newPassword) {
        throw new UnsupportedOperationException("setOwnPassword is not a literal");
    }

    @Override
    public NULL alterUser(
            NULL p,
            boolean ifExists,
            SimpleEither<StringPos<NULL>, Object> username,
            Boolean suspended,
            NULL homeDatabase,
            boolean removeHome,
            List<NULL> auths,
            List<NULL> systemAuthAttributes,
            boolean removeAllAuth,
            List<Object> removeAuths) {
        throw new UnsupportedOperationException("alterUser is not a literal");
    }

    @Override
    public NULL auth(String provider, List<NULL> nulls, NULL p) {
        throw new UnsupportedOperationException("auth is not a literal");
    }

    @Override
    public NULL authId(NULL s, Object id) {
        throw new UnsupportedOperationException("authId is not a literal");
    }

    @Override
    public NULL password(NULL p, Object password, boolean encrypted) {
        throw new UnsupportedOperationException("password is not a literal");
    }

    @Override
    public NULL passwordChangeRequired(NULL p, boolean changeRequired) {
        throw new UnsupportedOperationException("passwordChangeRequired is not a literal");
    }

    @Override
    public Object passwordExpression(Object password) {
        throw new UnsupportedOperationException("passwordExpression is not a literal");
    }

    @Override
    public Object passwordExpression(NULL s, NULL e, String password) {
        throw new UnsupportedOperationException("passwordExpression is not a literal");
    }

    @Override
    public NULL showUsers(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL where, boolean withAuth) {
        throw new UnsupportedOperationException("showUsers is not a literal");
    }

    @Override
    public NULL showCurrentUser(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL where) {
        throw new UnsupportedOperationException("showCurrentUser is not a literal");
    }

    // Privilege commands

    @Override
    public NULL showSupportedPrivileges(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        throw new UnsupportedOperationException("showSupportedPrivileges is not a literal");
    }

    @Override
    public NULL showAllPrivileges(
            NULL p, boolean asCommand, boolean asRevoke, NULL yieldExpr, NULL returnWithoutGraph, NULL where) {
        throw new UnsupportedOperationException("showAllPrivileges is not a literal");
    }

    @Override
    public NULL showRolePrivileges(
            NULL p,
            List<SimpleEither<StringPos<NULL>, Object>> roles,
            boolean asCommand,
            boolean asRevoke,
            NULL yieldExpr,
            NULL returnWithoutGraph,
            NULL where) {
        throw new UnsupportedOperationException("showRolePrivileges is not a literal");
    }

    @Override
    public NULL showUserPrivileges(
            NULL p,
            List<SimpleEither<StringPos<NULL>, Object>> users,
            boolean asCommand,
            boolean asRevoke,
            NULL yieldExpr,
            NULL returnWithoutGraph,
            NULL where) {
        throw new UnsupportedOperationException("showUserPrivileges is not a literal");
    }

    @Override
    public NULL grantPrivilege(NULL p, List<SimpleEither<StringPos<NULL>, Object>> roles, NULL privilege) {
        throw new UnsupportedOperationException("grantPrivilege is not a literal");
    }

    @Override
    public NULL denyPrivilege(NULL p, List<SimpleEither<StringPos<NULL>, Object>> roles, NULL privilege) {
        throw new UnsupportedOperationException("denyPrivilege is not a literal");
    }

    @Override
    public NULL revokePrivilege(
            NULL p,
            List<SimpleEither<StringPos<NULL>, Object>> roles,
            NULL privilege,
            boolean revokeGrant,
            boolean revokeDeny) {
        throw new UnsupportedOperationException("revokePrivilege is not a literal");
    }

    @Override
    public NULL databasePrivilege(NULL p, NULL action, NULL scope, List<NULL> qualifier, boolean immutable) {
        throw new UnsupportedOperationException("databasePrivilege is not a literal");
    }

    @Override
    public NULL dbmsPrivilege(NULL p, NULL action, List<NULL> qualifier, boolean immutable) {
        throw new UnsupportedOperationException("dbmsPrivilege is not a literal");
    }

    @Override
    public NULL graphPrivilege(
            NULL p, NULL action, NULL scope, NULL resource, List<NULL> qualifier, boolean immutable) {
        throw new UnsupportedOperationException("graphPrivilege is not a literal");
    }

    @Override
    public NULL loadPrivilege(
            NULL p, SimpleEither<String, Object> url, SimpleEither<String, Object> cidr, boolean immutable) {
        throw new UnsupportedOperationException("loadPrivilege is not a literal");
    }

    @Override
    public NULL privilegeAction(ActionType action) {
        throw new UnsupportedOperationException("privilegeAction is not a literal");
    }

    @Override
    public NULL propertiesResource(NULL p, List<String> property) {
        throw new UnsupportedOperationException("propertiesResource is not a literal");
    }

    @Override
    public NULL allPropertiesResource(NULL p) {
        throw new UnsupportedOperationException("allPropertiesResource is not a literal");
    }

    @Override
    public NULL labelsResource(NULL p, List<String> label) {
        throw new UnsupportedOperationException("labelsResource is not a literal");
    }

    @Override
    public NULL allLabelsResource(NULL p) {
        throw new UnsupportedOperationException("allLabelsResource is not a literal");
    }

    @Override
    public NULL databaseResource(NULL p) {
        throw new UnsupportedOperationException("databaseResource is not a literal");
    }

    @Override
    public NULL noResource(NULL p) {
        throw new UnsupportedOperationException("noResource is not a literal");
    }

    @Override
    public NULL labelQualifier(NULL p, String label) {
        throw new UnsupportedOperationException("labelQualifier is not a literal");
    }

    @Override
    public NULL allLabelsQualifier(NULL p) {
        throw new UnsupportedOperationException("allLabelsQualifier is not a literal");
    }

    @Override
    public NULL relationshipQualifier(NULL p, String relationshipType) {
        throw new UnsupportedOperationException("relationshipQualifier is not a literal");
    }

    @Override
    public NULL allRelationshipsQualifier(NULL p) {
        throw new UnsupportedOperationException("allRelationshipsQualifier is not a literal");
    }

    @Override
    public NULL elementQualifier(NULL p, String name) {
        throw new UnsupportedOperationException("elementQualifier is not a literal");
    }

    @Override
    public NULL allElementsQualifier(NULL p) {
        throw new UnsupportedOperationException("allElementsQualifier is not a literal");
    }

    @Override
    public NULL patternQualifier(List<NULL> qualifiers, Object variable, Object where) {
        throw new UnsupportedOperationException("patternQualifier is not a literal");
    }

    @Override
    public List<NULL> allQualifier() {
        throw new UnsupportedOperationException("allQualifier is not a literal");
    }

    @Override
    public List<NULL> allDatabasesQualifier() {
        throw new UnsupportedOperationException("allDatabasesQualifier is not a literal");
    }

    @Override
    public List<NULL> userQualifier(List<SimpleEither<StringPos<NULL>, Object>> users) {
        throw new UnsupportedOperationException("userQualifier is not a literal");
    }

    @Override
    public List<NULL> allUsersQualifier() {
        throw new UnsupportedOperationException("allUsersQualifier is not a literal");
    }

    @Override
    public List<NULL> functionQualifier(NULL p, List<String> functions) {
        throw new UnsupportedOperationException("functionQualifier is not a literal");
    }

    @Override
    public List<NULL> procedureQualifier(NULL p, List<String> procedures) {
        throw new UnsupportedOperationException("procedureQualifier is not a literal");
    }

    @Override
    public List<NULL> settingQualifier(NULL p, List<String> names) {
        throw new UnsupportedOperationException("settingQualifier is not a literal");
    }

    @Override
    public NULL graphScope(NULL p, List<NULL> graphNames, ScopeType scopeType) {
        throw new UnsupportedOperationException("graphScope is not a literal");
    }

    @Override
    public NULL databaseScope(NULL p, List<NULL> databaseNames, ScopeType scopeType) {
        throw new UnsupportedOperationException("databaseScope is not a literal");
    }

    // Server commands

    @Override
    public NULL enableServer(
            NULL p, SimpleEither<String, Object> serverName, SimpleEither<Map<String, Object>, Object> options) {
        throw new UnsupportedOperationException("enableServer is not a literal");
    }

    @Override
    public NULL alterServer(
            NULL p, SimpleEither<String, Object> serverName, SimpleEither<Map<String, Object>, Object> options) {
        throw new UnsupportedOperationException("alterServer is not a literal");
    }

    @Override
    public NULL renameServer(NULL p, SimpleEither<String, Object> serverName, SimpleEither<String, Object> newName) {
        throw new UnsupportedOperationException("renameServer is not a literal");
    }

    @Override
    public NULL dropServer(NULL p, SimpleEither<String, Object> serverName) {
        throw new UnsupportedOperationException("dropServer is not a literal");
    }

    @Override
    public NULL showServers(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        throw new UnsupportedOperationException("showServers is not a literal");
    }

    @Override
    public NULL deallocateServers(NULL p, boolean dryRun, List<SimpleEither<String, Object>> serverNames) {
        throw new UnsupportedOperationException("deallocateServers is not a literal");
    }

    @Override
    public NULL reallocateDatabases(NULL p, boolean dryRun) {
        throw new UnsupportedOperationException("reallocateDatabases is not a literal");
    }

    // Database commands

    @Override
    public NULL createDatabase(
            NULL p,
            boolean replace,
            NULL databaseName,
            boolean ifNotExists,
            NULL aNull,
            SimpleEither<Map<String, Object>, Object> options,
            Integer topologyPrimaries,
            Integer topologySecondaries) {
        throw new UnsupportedOperationException("createDatabase is not a literal");
    }

    @Override
    public NULL dropDatabase(
            NULL p, NULL databaseName, boolean ifExists, boolean composite, boolean dumpData, NULL wait) {
        throw new UnsupportedOperationException("dropDatabase is not a literal");
    }

    @Override
    public NULL alterDatabase(
            NULL p,
            NULL databaseName,
            boolean ifExists,
            AccessType accessType,
            Integer topologyPrimaries,
            Integer topologySecondaries,
            Map<String, Object> options,
            Set<String> optionsToRemove,
            NULL waitClause) {
        throw new UnsupportedOperationException("alterDatabase is not a literal");
    }

    @Override
    public NULL showDatabase(NULL p, NULL aNull, NULL yieldExpr, NULL returnWithoutGraph, NULL where) {
        throw new UnsupportedOperationException("showDatabase is not a literal");
    }

    @Override
    public NULL createCompositeDatabase(
            NULL p,
            boolean replace,
            NULL compositeDatabaseName,
            boolean ifNotExists,
            SimpleEither<Map<String, Object>, Object> options,
            NULL wait) {
        throw new UnsupportedOperationException("createCompositeDatabase is not a literal");
    }

    @Override
    public NULL databaseScope(NULL p, NULL name, boolean isDefault, boolean isHome) {
        throw new UnsupportedOperationException("databaseScope is not a literal");
    }

    @Override
    public NULL startDatabase(NULL p, NULL databaseName, NULL wait) {
        throw new UnsupportedOperationException("startDatabase is not a literal");
    }

    @Override
    public NULL stopDatabase(NULL p, NULL databaseName, NULL wait) {
        throw new UnsupportedOperationException("stopDatabase is not a literal");
    }

    @Override
    public NULL wait(boolean wait, long seconds) {
        throw new UnsupportedOperationException("wait is not a literal");
    }

    @Override
    public NULL createLocalDatabaseAlias(
            NULL p,
            boolean replace,
            NULL aliasName,
            NULL targetName,
            boolean ifNotExists,
            SimpleEither<Map<String, Object>, Object> properties) {
        throw new UnsupportedOperationException("createLocalDatabaseAlias is not a literal");
    }

    @Override
    public NULL createRemoteDatabaseAlias(
            NULL p,
            boolean replace,
            NULL aliasName,
            NULL targetName,
            boolean ifNotExists,
            SimpleEither<String, Object> url,
            SimpleEither<StringPos<NULL>, Object> username,
            Object password,
            SimpleEither<Map<String, Object>, Object> driverSettings,
            SimpleEither<Map<String, Object>, Object> properties) {
        throw new UnsupportedOperationException("createRemoteDatabaseAlias is not a literal");
    }

    @Override
    public NULL alterLocalDatabaseAlias(
            NULL p,
            NULL aliasName,
            NULL targetName,
            boolean ifExists,
            SimpleEither<Map<String, Object>, Object> properties) {
        throw new UnsupportedOperationException("alterLocalDatabaseAlias is not a literal");
    }

    @Override
    public NULL alterRemoteDatabaseAlias(
            NULL p,
            NULL aliasName,
            NULL targetName,
            boolean ifExists,
            SimpleEither<String, Object> url,
            SimpleEither<StringPos<NULL>, Object> username,
            Object password,
            SimpleEither<Map<String, Object>, Object> driverSettings,
            SimpleEither<Map<String, Object>, Object> properties) {
        throw new UnsupportedOperationException("alterRemoteDatabaseAlias is not a literal");
    }

    @Override
    public NULL dropAlias(NULL p, NULL aliasName, boolean ifExists) {

        throw new UnsupportedOperationException("dropAlias is not a literal");
    }

    @Override
    public NULL showAliases(NULL p, NULL aliasName, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        throw new UnsupportedOperationException("showAliases is not a literal");
    }

    @Override
    public void addDeprecatedIdentifierUnicodeNotification(NULL p, Character character, String identifier) {
        throw new UnsupportedOperationException("This deprecation shouldn't occur in a literal.");
    }

    @Override
    public Object newVariable(NULL p, String name) {
        throw new UnsupportedOperationException("newVariable is not a literal");
    }

    @Override
    public Object newParameter(NULL p, Object v, ParameterType type) {
        throw new UnsupportedOperationException("newParameter is not a literal");
    }

    @Override
    public Object newParameter(NULL p, String offset, ParameterType type) {
        throw new UnsupportedOperationException("newParameter is not a literal");
    }

    @Override
    public Object newSensitiveStringParameter(NULL p, Object v) {
        throw new UnsupportedOperationException("newSensitiveStringParameter is not a literal");
    }

    @Override
    public Object newSensitiveStringParameter(NULL p, String offset) {
        throw new UnsupportedOperationException("newSensitiveStringParameter is not a literal");
    }

    @Override
    public Object newDouble(NULL p, String image) {
        return Double.valueOf(image);
    }

    @Override
    public Object newDecimalInteger(NULL p, String image, boolean negated) {
        try {
            long x = Long.parseLong(image);
            return negated ? -x : x;
        } catch (NumberFormatException e) {
            if (negated && LONG_MIN_VALUE_DECIMAL_STRING.equals(image)) {
                return Long.MIN_VALUE;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Object newHexInteger(NULL p, String image, boolean negated) {
        try {
            long x = Long.parseLong(image.substring(2), 16);
            return negated ? -x : x;
        } catch (NumberFormatException e) {
            if (negated && LONG_MIN_VALUE_HEXADECIMAL_STRING.equals(image)) {
                return Long.MIN_VALUE;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Object newOctalInteger(NULL p, String image, boolean negated) {
        try {
            long x;
            if (image.charAt(1) == 'o') {
                x = Long.parseLong(image.substring(2), 8);
            } else {
                x = Long.parseLong(image, 8);
            }
            return negated ? -x : x;
        } catch (NumberFormatException e) {
            if (negated
                    && (LONG_MIN_VALUE_OCTAL_STRING.equals(image)
                            || LONG_MIN_VALUE_OCTAL_STRING_OLD_SYNTAX.equals(image))) {
                return Long.MIN_VALUE;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Object newString(NULL s, NULL e, String image) {
        return image;
    }

    @Override
    public Object newTrueLiteral(NULL p) {
        return Boolean.TRUE;
    }

    @Override
    public Object newFalseLiteral(NULL p) {
        return Boolean.FALSE;
    }

    @Override
    public Object newInfinityLiteral(NULL p) {
        return Double.POSITIVE_INFINITY;
    }

    @Override
    public Object newNaNLiteral(NULL p) {
        return Double.NaN;
    }

    @Override
    public Object newNullLiteral(NULL p) {
        return null;
    }

    @Override
    public Object listLiteral(NULL p, List<Object> values) {
        return values;
    }

    @Override
    public Object mapLiteral(NULL p, List<StringPos<NULL>> keys, List<Object> values) {
        HashMap<String, Object> x = new HashMap<>();
        Iterator<StringPos<NULL>> keyIterator = keys.iterator();
        Iterator<Object> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
            x.put(keyIterator.next().string, valueIterator.next());
        }
        return x;
    }

    @Override
    public Object property(Object subject, StringPos<NULL> propertyKeyName) {
        throw new UnsupportedOperationException("property is not a literal");
    }

    @Override
    public Object or(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("or is not a literal");
    }

    @Override
    public Object xor(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("xor is not a literal");
    }

    @Override
    public Object and(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("and is not a literal");
    }

    @Override
    public NULL labelConjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        throw new UnsupportedOperationException("labelConjunction is not a literal");
    }

    @Override
    public NULL labelDisjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        throw new UnsupportedOperationException("labelDisjunction is not a literal");
    }

    @Override
    public NULL labelNegation(NULL p, NULL e, boolean containsIs) {
        throw new UnsupportedOperationException("labelNegation is not a literal");
    }

    @Override
    public NULL labelWildcard(NULL p, boolean containsIs) {
        throw new UnsupportedOperationException("labelWildcard is not a literal");
    }

    @Override
    public NULL labelLeaf(NULL p, String e, NULL entityType, boolean containsIs) {
        throw new UnsupportedOperationException("labelAtom is not a literal");
    }

    @Override
    public NULL labelColonConjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        throw new UnsupportedOperationException("labelColonConjunction is not a literal");
    }

    @Override
    public NULL labelColonDisjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        throw new UnsupportedOperationException("labelColonDisjunction is not a literal");
    }

    @Override
    public Object labelExpressionPredicate(Object subject, NULL exp) {
        throw new UnsupportedOperationException("labelExpressionPredicate is not a literal");
    }

    @Override
    public Object ands(List<Object> exprs) {
        throw new UnsupportedOperationException("ands is not a literal");
    }

    @Override
    public Object not(NULL p, Object e) {
        throw new UnsupportedOperationException("not is not a literal");
    }

    @Override
    public Object plus(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("plus is not a literal");
    }

    @Override
    public Object concatenate(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("concatenation is not a literal");
    }

    @Override
    public Object minus(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("minus is not a literal");
    }

    @Override
    public Object multiply(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("multiply is not a literal");
    }

    @Override
    public Object divide(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("divide is not a literal");
    }

    @Override
    public Object modulo(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("modulo is not a literal");
    }

    @Override
    public Object pow(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("pow is not a literal");
    }

    @Override
    public Object unaryPlus(Object e) {
        throw new UnsupportedOperationException("unaryPlus is not a literal");
    }

    @Override
    public Object unaryPlus(NULL p, Object e) {
        throw new UnsupportedOperationException("unaryPlus is not a literal");
    }

    @Override
    public Object unaryMinus(NULL p, Object e) {
        throw new UnsupportedOperationException("unaryMinus is not a literal");
    }

    @Override
    public Object eq(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("eq is not a literal");
    }

    @Override
    public Object neq(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("neq is not a literal");
    }

    @Override
    public Object neq2(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("neq2 is not a literal");
    }

    @Override
    public Object lte(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("lte is not a literal");
    }

    @Override
    public Object gte(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("gte is not a literal");
    }

    @Override
    public Object lt(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("lt is not a literal");
    }

    @Override
    public Object gt(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("gt is not a literal");
    }

    @Override
    public Object regeq(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("regeq is not a literal");
    }

    @Override
    public Object startsWith(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("startsWith is not a literal");
    }

    @Override
    public Object endsWith(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("endsWith is not a literal");
    }

    @Override
    public Object contains(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("contains is not a literal");
    }

    @Override
    public Object in(NULL p, Object lhs, Object rhs) {
        throw new UnsupportedOperationException("in is not a literal");
    }

    @Override
    public Object isNull(NULL p, Object e) {
        throw new UnsupportedOperationException("isNull is not a literal");
    }

    @Override
    public Object isNotNull(NULL p, Object e) {
        throw new UnsupportedOperationException("isNotNull is not a literal");
    }

    @Override
    public Object isTyped(NULL p, Object e, ParserCypherTypeName typeName) {
        throw new UnsupportedOperationException("isTyped is not a literal");
    }

    @Override
    public Object isNotTyped(NULL p, Object e, ParserCypherTypeName typeName) {
        throw new UnsupportedOperationException("isNotTyped is not a literal");
    }

    @Override
    public Object isNormalized(NULL p, Object e, ParserNormalForm normalForm) {
        throw new UnsupportedOperationException("isNormalized is not a literal");
    }

    @Override
    public Object isNotNormalized(NULL p, Object e, ParserNormalForm normalForm) {
        throw new UnsupportedOperationException("isNotNormalized is not a literal");
    }

    @Override
    public Object listLookup(Object list, Object index) {
        throw new UnsupportedOperationException("listLookup is not a literal");
    }

    @Override
    public Object listSlice(NULL p, Object list, Object start, Object end) {
        throw new UnsupportedOperationException("listSlice is not a literal");
    }

    @Override
    public Object newCountStar(NULL p) {
        throw new UnsupportedOperationException("newCountStar is not a literal");
    }

    @Override
    public Object functionInvocation(
            NULL p,
            NULL functionNamePosition,
            List<String> namespace,
            String name,
            boolean distinct,
            List<Object> arguments,
            boolean calledFromUseClause) {
        if (namespace.isEmpty()) {
            switch (name.toLowerCase(Locale.ROOT)) {
                case "date":
                    return createTemporalValue(arguments, name, DateValue::now, DateValue::parse, DateValue::build);
                case "datetime":
                    return createTemporalValue(
                            arguments,
                            name,
                            DateTimeValue::now,
                            s -> DateTimeValue.parse(s, () -> DEFAULT_ZONE_ID),
                            DateTimeValue::build);
                case "time":
                    return createTemporalValue(
                            arguments,
                            name,
                            TimeValue::now,
                            s -> TimeValue.parse(s, () -> DEFAULT_ZONE_ID),
                            TimeValue::build);
                case "localtime":
                    return createTemporalValue(
                            arguments, name, LocalTimeValue::now, LocalTimeValue::parse, LocalTimeValue::build);
                case "localdatetime":
                    return createTemporalValue(
                            arguments,
                            name,
                            LocalDateTimeValue::now,
                            LocalDateTimeValue::parse,
                            LocalDateTimeValue::build);
                case "duration":
                    return createDurationValue(arguments);
                case "point":
                    return createPoint(arguments);
                default:
                    throw new UnsupportedOperationException("functionInvocation (" + name + ") is not a literal");
            }
        }
        throw new UnsupportedOperationException("functionInvocation is not a literal");
    }

    private static PointValue createPoint(List<Object> arguments) {
        if (arguments.size() == 1) {
            Object point = arguments.get(0);
            if (point == null) {
                return null;
            } else if (point instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, ?> pointAsMap = (Map<String, ?>) point;
                return PointValue.fromMap(asMapValue(pointAsMap));
            } else {
                throw new IllegalArgumentException(
                        "Function `point` did not get expected argument. Expected a string or map input but got "
                                + point.getClass().getSimpleName() + ".");
            }
        } else {
            throw new IllegalArgumentException(
                    "Function `point` did not get expected number of arguments: expected 1 argument, got "
                            + arguments.size() + " arguments.");
        }
    }

    private static <T> T createTemporalValue(
            List<Object> arguments,
            String functionName,
            Function<Clock, T> onEmpty,
            Function<String, T> onString,
            BiFunction<MapValue, Supplier<ZoneId>, T> onMap) {
        if (arguments.isEmpty()) {
            return onEmpty.apply(Clock.system(DEFAULT_ZONE_ID));
        } else if (arguments.size() == 1) {
            Object date = arguments.get(0);
            if (date == null) {
                return null;
            } else if (date instanceof String) {
                return onString.apply((String) date);
            } else if (date instanceof Map) {
                @SuppressWarnings("unchecked")
                MapValue dateMap = asMapValue((Map<String, ?>) date);
                return onMap.apply(dateMap, () -> DEFAULT_ZONE_ID);
            }
        }

        throw new IllegalArgumentException("Function `" + functionName
                + "` did not get expected number of arguments: expected 0 or 1 argument, got " + arguments.size()
                + " arguments.");
    }

    private static DurationValue createDurationValue(List<Object> arguments) {
        if (arguments.size() == 1) {
            Object duration = arguments.get(0);
            if (duration instanceof String) {
                return DurationValue.parse((String) duration);
            } else if (duration instanceof Map) {
                @SuppressWarnings("unchecked")
                MapValue dateMap = asMapValue((Map<String, ?>) duration);
                return DurationValue.build(dateMap);
            }
        }

        throw new IllegalArgumentException(
                "Function `duration` did not get expected number of arguments: expected 1 argument, got "
                        + arguments.size() + " arguments.");
    }

    @Override
    public Object listComprehension(NULL p, Object v, Object list, Object where, Object projection) {
        throw new UnsupportedOperationException("listComprehension is not a literal");
    }

    @Override
    public Object patternComprehension(
            NULL p, NULL relationshipPatternPosition, Object v, NULL aNull, Object where, Object projection) {
        throw new UnsupportedOperationException("patternComprehension is not a literal");
    }

    @Override
    public Object reduceExpression(NULL p, Object acc, Object accExpr, Object v, Object list, Object innerExpr) {
        throw new UnsupportedOperationException("reduceExpression is not a literal");
    }

    @Override
    public Object allExpression(NULL p, Object v, Object list, Object where) {
        throw new UnsupportedOperationException("allExpression is not a literal");
    }

    @Override
    public Object anyExpression(NULL p, Object v, Object list, Object where) {
        throw new UnsupportedOperationException("anyExpression is not a literal");
    }

    @Override
    public Object noneExpression(NULL p, Object v, Object list, Object where) {
        throw new UnsupportedOperationException("noneExpression is not a literal");
    }

    @Override
    public Object singleExpression(NULL p, Object v, Object list, Object where) {
        throw new UnsupportedOperationException("singleExpression is not a literal");
    }

    @Override
    public Object normalizeExpression(NULL p, Object i, ParserNormalForm normalForm) {
        throw new UnsupportedOperationException("normalizeExpression is not a literal");
    }

    @Override
    public Object trimFunction(
            NULL p, ParserTrimSpecification trimSpec, Object trimCharacterString, Object trimSource) {
        throw new UnsupportedOperationException("trimFunction is not a literal");
    }

    @Override
    public Object patternExpression(NULL p, NULL aNull) {
        throw new UnsupportedOperationException("patternExpression is not a literal");
    }

    @Override
    public Object existsExpression(NULL p, NULL matchMode, List<NULL> nulls, NULL q, NULL where) {
        throw new UnsupportedOperationException("existsExpression is not a literal");
    }

    @Override
    public Object countExpression(NULL p, NULL matchMode, List<NULL> nulls, NULL q, NULL where) {
        throw new UnsupportedOperationException("countExpression is not a literal");
    }

    @Override
    public Object collectExpression(NULL p, NULL q) {
        throw new UnsupportedOperationException("collectExpression is not a literal");
    }

    @Override
    public Object mapProjection(NULL p, Object v, List<NULL> nulls) {
        throw new UnsupportedOperationException("mapProjection is not a literal");
    }

    @Override
    public NULL mapProjectionLiteralEntry(StringPos<NULL> property, Object value) {
        throw new UnsupportedOperationException("mapProjectionLiteralEntry is not a literal");
    }

    @Override
    public NULL mapProjectionProperty(StringPos<NULL> property) {
        throw new UnsupportedOperationException("mapProjectionProperty is not a literal");
    }

    @Override
    public NULL mapProjectionVariable(Object v) {
        throw new UnsupportedOperationException("mapProjectionVariable is not a literal");
    }

    @Override
    public NULL mapProjectionAll(NULL p) {
        throw new UnsupportedOperationException("mapProjectionAll is not a literal");
    }

    @Override
    public Object caseExpression(NULL p, Object e, List<Object> whens, List<Object> thens, Object elze) {
        throw new UnsupportedOperationException("caseExpression is not a literal");
    }

    @Override
    public NULL inputPosition(int offset, int line, int column) {
        return null;
    }

    @Override
    public NULL nodeType() {
        return null;
    }

    @Override
    public NULL relationshipType() {
        return null;
    }

    @Override
    public NULL nodeOrRelationshipType() {
        return null;
    }

    @Override
    public NULL databaseName(NULL pos, List<String> names) {
        throw new UnsupportedOperationException("databaseName is not a literal");
    }

    @Override
    public NULL databaseName(Object param) {
        throw new UnsupportedOperationException("databaseName is not a literal");
    }

    private static MapValue asMapValue(Map<String, ?> map) {
        int size = map.size();
        if (size == 0) {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder(size);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            builder.add(entry.getKey(), Values.of(entry.getValue()));
        }
        return builder.build();
    }
}
