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
package org.neo4j.cypher.internal.ast.factory.empty;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory.NULL;
import org.neo4j.cypher.internal.ast.factory.AccessType;
import org.neo4j.cypher.internal.ast.factory.ActionType;
import org.neo4j.cypher.internal.ast.factory.CallInTxsOnErrorBehaviourType;
import org.neo4j.cypher.internal.ast.factory.ConstraintType;
import org.neo4j.cypher.internal.ast.factory.ConstraintVersion;
import org.neo4j.cypher.internal.ast.factory.CreateIndexTypes;
import org.neo4j.cypher.internal.ast.factory.HintIndexType;
import org.neo4j.cypher.internal.ast.factory.ParameterType;
import org.neo4j.cypher.internal.ast.factory.ParserCypherTypeName;
import org.neo4j.cypher.internal.ast.factory.ParserNormalForm;
import org.neo4j.cypher.internal.ast.factory.ParserTrimSpecification;
import org.neo4j.cypher.internal.ast.factory.ScopeType;
import org.neo4j.cypher.internal.ast.factory.ShowCommandFilterTypes;
import org.neo4j.cypher.internal.ast.factory.SimpleEither;

/**
 * Returns null on all AST methods, can be useful to analyse cypher syntax without materialising the AST.
 */
public class NullAstFactory
        implements ASTFactory<
                NULL, // STATEMENTS
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
                NULL, // EXPRESSION,
                NULL, // LABEL_EXPRESSION,
                NULL, // FUNCTION_INVOCATION extends EXPRESSION,
                NULL, // PARAMETER extends EXPRESSION,
                NULL, // VARIABLE extends EXPRESSION,
                NULL, // PROPERTY extends EXPRESSION,
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
                NULL, // MATCH MODE
                NULL // PATTERN_ELEMENT
        > {

    @Override
    public NULL statements(List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL newVariable(NULL p, String name) {
        return null;
    }

    @Override
    public NULL newParameter(NULL p, NULL v, ParameterType type) {
        return null;
    }

    @Override
    public NULL newParameter(NULL p, String offset, ParameterType type) {
        return null;
    }

    @Override
    public NULL newSensitiveStringParameter(NULL p, NULL v) {
        return null;
    }

    @Override
    public NULL newSensitiveStringParameter(NULL p, String offset) {
        return null;
    }

    @Override
    public NULL newDouble(NULL p, String image) {
        return null;
    }

    @Override
    public NULL newDecimalInteger(NULL p, String image, boolean negated) {
        return null;
    }

    @Override
    public NULL newHexInteger(NULL p, String image, boolean negated) {
        return null;
    }

    @Override
    public NULL newOctalInteger(NULL p, String image, boolean negated) {
        return null;
    }

    @Override
    public NULL newString(NULL s, NULL e, String image) {
        return null;
    }

    @Override
    public NULL newTrueLiteral(NULL p) {
        return null;
    }

    @Override
    public NULL newFalseLiteral(NULL p) {
        return null;
    }

    @Override
    public NULL newInfinityLiteral(NULL p) {
        return null;
    }

    @Override
    public NULL newNaNLiteral(NULL p) {
        return null;
    }

    @Override
    public NULL newNullLiteral(NULL p) {
        return null;
    }

    @Override
    public NULL listLiteral(NULL p, List<NULL> values) {
        return null;
    }

    @Override
    public NULL mapLiteral(NULL p, List<StringPos<NULL>> keys, List<NULL> values) {
        return null;
    }

    @Override
    public NULL property(NULL subject, StringPos<NULL> propertyKeyName) {
        return null;
    }

    @Override
    public NULL or(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL xor(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL and(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL labelConjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelDisjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelNegation(NULL p, NULL e, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelWildcard(NULL p, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelLeaf(NULL p, String e, NULL entityType, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelColonConjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelColonDisjunction(NULL p, NULL lhs, NULL rhs, boolean containsIs) {
        return null;
    }

    @Override
    public NULL labelExpressionPredicate(NULL subject, NULL exp) {
        return null;
    }

    @Override
    public NULL ands(List<NULL> exprs) {
        return null;
    }

    @Override
    public NULL not(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL plus(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL concatenate(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL minus(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL multiply(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL divide(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL modulo(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL pow(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL unaryPlus(NULL e) {
        return null;
    }

    @Override
    public NULL unaryPlus(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL unaryMinus(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL eq(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL neq(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL neq2(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL lte(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL gte(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL lt(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL gt(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL regeq(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL startsWith(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL endsWith(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL contains(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL in(NULL p, NULL lhs, NULL rhs) {
        return null;
    }

    @Override
    public NULL isNull(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL isNotNull(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL isTyped(NULL p, NULL e, ParserCypherTypeName typeName) {
        return null;
    }

    @Override
    public NULL isNotTyped(NULL p, NULL e, ParserCypherTypeName typeName) {
        return null;
    }

    @Override
    public NULL isNormalized(NULL p, NULL e, ParserNormalForm normalForm) {
        return null;
    }

    @Override
    public NULL isNotNormalized(NULL p, NULL e, ParserNormalForm normalForm) {
        return null;
    }

    @Override
    public NULL listLookup(NULL list, NULL index) {
        return null;
    }

    @Override
    public NULL listSlice(NULL p, NULL list, NULL start, NULL end) {
        return null;
    }

    @Override
    public NULL newCountStar(NULL p) {
        return null;
    }

    @Override
    public NULL functionInvocation(
            NULL p,
            NULL functionNamePosition,
            List<String> namespace,
            String name,
            boolean distinct,
            List<NULL> arguments,
            boolean calledFromUseClause) {
        return null;
    }

    @Override
    public NULL listComprehension(NULL p, NULL v, NULL list, NULL where, NULL projection) {
        return null;
    }

    @Override
    public NULL patternComprehension(
            NULL p, NULL relationshipPatternPosition, NULL v, NULL aNull, NULL where, NULL projection) {
        return null;
    }

    @Override
    public NULL reduceExpression(NULL p, NULL acc, NULL accExpr, NULL v, NULL list, NULL innerExpr) {
        return null;
    }

    @Override
    public NULL allExpression(NULL p, NULL v, NULL list, NULL where) {
        return null;
    }

    @Override
    public NULL anyExpression(NULL p, NULL v, NULL list, NULL where) {
        return null;
    }

    @Override
    public NULL noneExpression(NULL p, NULL v, NULL list, NULL where) {
        return null;
    }

    @Override
    public NULL singleExpression(NULL p, NULL v, NULL list, NULL where) {
        return null;
    }

    @Override
    public NULL normalizeExpression(NULL p, NULL i, ParserNormalForm normalForm) {
        return null;
    }

    @Override
    public NULL trimFunction(NULL p, ParserTrimSpecification trimSpec, NULL trimCharacterString, NULL trimSource) {
        return null;
    }

    @Override
    public NULL patternExpression(NULL p, NULL aNull) {
        return null;
    }

    @Override
    public NULL existsExpression(NULL p, NULL matchMode, List<NULL> nulls, NULL q, NULL where) {
        return null;
    }

    @Override
    public NULL countExpression(NULL p, NULL matchMode, List<NULL> nulls, NULL q, NULL where) {
        return null;
    }

    @Override
    public NULL collectExpression(NULL p, NULL q) {
        return null;
    }

    @Override
    public NULL mapProjection(NULL p, NULL v, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL mapProjectionLiteralEntry(StringPos<NULL> property, NULL value) {
        return null;
    }

    @Override
    public NULL mapProjectionProperty(StringPos<NULL> property) {
        return null;
    }

    @Override
    public NULL mapProjectionVariable(NULL v) {
        return null;
    }

    @Override
    public NULL mapProjectionAll(NULL p) {
        return null;
    }

    @Override
    public NULL caseExpression(NULL p, NULL e, List<NULL> whens, List<NULL> thens, NULL elze) {
        return null;
    }

    @Override
    public NULL inputPosition(int offset, int line, int column) {
        return null;
    }

    @Override
    public NULL newSingleQuery(NULL p, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL newSingleQuery(List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL newUnion(NULL p, NULL lhs, NULL rhs, boolean all) {
        return null;
    }

    @Override
    public NULL directUseClause(NULL p, NULL aNull) {
        return null;
    }

    @Override
    public NULL functionUseClause(NULL p, NULL function) {
        return null;
    }

    @Override
    public NULL newFinishClause(NULL p) {
        return null;
    }

    @Override
    public NULL newReturnClause(
            NULL p,
            boolean distinct,
            NULL aNull,
            List<NULL> order,
            NULL orderPos,
            NULL skip,
            NULL skipPosition,
            NULL limit,
            NULL limitPosition) {
        return null;
    }

    @Override
    public NULL newReturnItems(NULL p, boolean returnAll, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL newReturnItem(NULL p, NULL e, NULL v) {
        return null;
    }

    @Override
    public NULL newReturnItem(NULL p, NULL e, int eStartOffset, int eEndOffset) {
        return null;
    }

    @Override
    public NULL orderDesc(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL orderAsc(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL whereClause(NULL p, NULL e) {
        return null;
    }

    @Override
    public NULL withClause(NULL p, NULL aNull, NULL aNull2) {
        return null;
    }

    @Override
    public NULL matchClause(
            NULL p,
            boolean optional,
            NULL matchMode,
            List<NULL> nulls,
            NULL patternPos,
            List<NULL> nulls2,
            NULL aNull) {
        return null;
    }

    @Override
    public NULL usingIndexHint(
            NULL p, NULL v, String labelOrRelType, List<String> properties, boolean seekOnly, HintIndexType indexType) {
        return null;
    }

    @Override
    public NULL usingJoin(NULL p, List<NULL> joinVariables) {
        return null;
    }

    @Override
    public NULL usingScan(NULL p, NULL v, String labelOrRelType) {
        return null;
    }

    @Override
    public NULL createClause(NULL p, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL insertClause(NULL p, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL setClause(NULL p, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL setProperty(NULL aNull, NULL value) {
        return null;
    }

    @Override
    public NULL setDynamicProperty(NULL aNull, NULL value) {
        return null;
    }

    @Override
    public NULL setVariable(NULL aNull, NULL value) {
        return null;
    }

    @Override
    public NULL addAndSetVariable(NULL aNull, NULL value) {
        return null;
    }

    @Override
    public NULL setLabels(NULL aNull, List<StringPos<NULL>> value, boolean containsIs) {
        return null;
    }

    @Override
    public NULL removeClause(NULL p, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL removeProperty(NULL aNull) {
        return null;
    }

    @Override
    public NULL removeDynamicProperty(NULL aNull) {
        return null;
    }

    @Override
    public NULL removeLabels(NULL aNull, List<StringPos<NULL>> labels, boolean containsIs) {
        return null;
    }

    @Override
    public NULL deleteClause(NULL p, boolean detach, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL unwindClause(NULL p, NULL e, NULL v) {
        return null;
    }

    @Override
    public NULL mergeClause(
            NULL p, NULL aNull, List<NULL> nulls, List<MergeActionType> actionTypes, List<NULL> positions) {
        return null;
    }

    @Override
    public NULL callClause(
            NULL p,
            NULL namespacePosition,
            NULL procedureNamePosition,
            NULL procedureResultPosition,
            List<String> namespace,
            String name,
            List<NULL> arguments,
            boolean yieldAll,
            List<NULL> nulls,
            NULL aNull) {
        return null;
    }

    @Override
    public NULL callResultItem(NULL p, String name, NULL v) {
        return null;
    }

    @Override
    public NULL patternWithSelector(NULL selector, NULL patternPart) {
        return null;
    }

    @Override
    public NULL namedPattern(NULL v, NULL aNull) {
        return null;
    }

    @Override
    public NULL shortestPathPattern(NULL p, NULL aNull) {
        return null;
    }

    @Override
    public NULL allShortestPathsPattern(NULL p, NULL aNull) {
        return null;
    }

    @Override
    public NULL pathPattern(NULL aNull) {
        return null;
    }

    @Override
    public NULL insertPathPattern(List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL patternElement(List<NULL> atoms) {
        return null;
    }

    @Override
    public NULL anyPathSelector(String count, NULL pCount, NULL position) {
        return null;
    }

    @Override
    public NULL allPathSelector(NULL position) {
        return null;
    }

    @Override
    public NULL anyShortestPathSelector(String count, NULL pCount, NULL position) {
        return null;
    }

    @Override
    public NULL allShortestPathSelector(NULL position) {
        return null;
    }

    @Override
    public NULL shortestGroupsSelector(String count, NULL countPosition, NULL position) {
        return null;
    }

    @Override
    public NULL nodePattern(NULL p, NULL v, NULL aNull, NULL properties, NULL predicate) {
        return null;
    }

    @Override
    public NULL relationshipPattern(
            NULL p,
            boolean left,
            boolean right,
            NULL v,
            NULL labelExpressions,
            NULL aNull,
            NULL properties,
            NULL predicate) {
        return null;
    }

    @Override
    public NULL pathLength(NULL p, NULL pMin, NULL pMax, String minLength, String maxLength) {
        return null;
    }

    @Override
    public NULL intervalPathQuantifier(
            NULL p, NULL posLowerBound, NULL posUpperBound, String lowerBound, String upperBound) {
        return null;
    }

    @Override
    public NULL fixedPathQuantifier(NULL p, NULL valuePos, String value) {
        return null;
    }

    @Override
    public NULL plusPathQuantifier(NULL p) {
        return null;
    }

    @Override
    public NULL starPathQuantifier(NULL p) {
        return null;
    }

    @Override
    public NULL repeatableElements(NULL p) {
        return null;
    }

    @Override
    public NULL differentRelationships(NULL p) {
        return null;
    }

    @Override
    public NULL parenthesizedPathPattern(NULL p, NULL internalPattern, NULL where, NULL quantifier) {
        return null;
    }

    @Override
    public NULL quantifiedRelationship(NULL rel, NULL quantifier) {
        return null;
    }

    @Override
    public NULL loadCsvClause(NULL p, boolean headers, NULL source, NULL v, String fieldTerminator) {
        return null;
    }

    @Override
    public NULL foreachClause(NULL p, NULL v, NULL list, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL subqueryClause(NULL p, NULL subquery, NULL inTransactions) {
        return null;
    }

    @Override
    public NULL subqueryInTransactionsParams(
            NULL p, NULL batchParams, NULL concurrencyParams, NULL errorParams, NULL reportParams) {
        return null;
    }

    @Override
    public NULL subqueryInTransactionsBatchParameters(NULL p, NULL batchSize) {
        return null;
    }

    @Override
    public NULL subqueryInTransactionsConcurrencyParameters(NULL p, NULL concurrency) {
        return null;
    }

    @Override
    public NULL subqueryInTransactionsErrorParameters(NULL p, CallInTxsOnErrorBehaviourType onErrorBehaviour) {
        return null;
    }

    @Override
    public NULL subqueryInTransactionsReportParameters(NULL p, NULL v) {
        return null;
    }

    @Override
    public NULL useGraph(NULL aNull, NULL aNull2) {
        return null;
    }

    @Override
    public NULL yieldClause(
            NULL p,
            boolean returnAll,
            List<NULL> nulls,
            NULL returnItemsPosition,
            List<NULL> orderBy,
            NULL orderPos,
            NULL skip,
            NULL skipPosition,
            NULL limit,
            NULL limitPosition,
            NULL aNull) {
        return null;
    }

    @Override
    public NULL showIndexClause(
            NULL p, ShowCommandFilterTypes indexType, boolean brief, boolean verbose, NULL aNull, NULL yieldClause) {
        return null;
    }

    @Override
    public NULL showConstraintClause(
            NULL p,
            ShowCommandFilterTypes constraintType,
            boolean brief,
            boolean verbose,
            NULL aNull,
            NULL yieldClause) {
        return null;
    }

    @Override
    public NULL showProcedureClause(NULL p, boolean currentUser, String user, NULL aNull, NULL yieldClause) {
        return null;
    }

    @Override
    public NULL showFunctionClause(
            NULL p,
            ShowCommandFilterTypes functionType,
            boolean currentUser,
            String user,
            NULL aNull,
            NULL yieldClause) {
        return null;
    }

    @Override
    public NULL showTransactionsClause(NULL p, SimpleEither<List<String>, NULL> ids, NULL aNull, NULL yieldClause) {
        return null;
    }

    @Override
    public NULL terminateTransactionsClause(
            NULL p, SimpleEither<List<String>, NULL> ids, NULL where, NULL yieldClause) {
        return null;
    }

    @Override
    public NULL showSettingsClause(NULL p, SimpleEither<List<String>, NULL> ids, NULL where, NULL yieldClause) {
        return null;
    }

    @Override
    public NULL turnYieldToWith(NULL yieldClause) {
        return null;
    }

    @Override
    public NULL createConstraint(
            NULL p,
            ConstraintType constraintType,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<NULL>, NULL> constraintName,
            NULL aNull,
            StringPos<NULL> label,
            List<NULL> nulls,
            ParserCypherTypeName propertyType,
            SimpleEither<Map<String, NULL>, NULL> options,
            boolean containsOn,
            ConstraintVersion constraintVersion) {
        return null;
    }

    @Override
    public NULL dropConstraint(NULL p, SimpleEither<StringPos<NULL>, NULL> name, boolean ifExists) {
        return null;
    }

    @Override
    public NULL dropConstraint(
            NULL p, ConstraintType constraintType, NULL aNull, StringPos<NULL> label, List<NULL> nulls) {
        return null;
    }

    @Override
    public NULL createIndexWithOldSyntax(NULL p, StringPos<NULL> label, List<StringPos<NULL>> properties) {
        return null;
    }

    @Override
    public NULL createLookupIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, NULL> indexName,
            NULL aNull,
            StringPos<NULL> functionName,
            NULL functionParameter,
            SimpleEither<Map<String, NULL>, NULL> options) {
        return null;
    }

    @Override
    public NULL createIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, NULL> indexName,
            NULL aNull,
            StringPos<NULL> label,
            List<NULL> nulls,
            SimpleEither<Map<String, NULL>, NULL> options,
            CreateIndexTypes indexType) {
        return null;
    }

    @Override
    public NULL createFulltextIndex(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<NULL>, NULL> indexName,
            NULL aNull,
            List<StringPos<NULL>> labels,
            List<NULL> nulls,
            SimpleEither<Map<String, NULL>, NULL> options) {
        return null;
    }

    @Override
    public NULL dropIndex(NULL p, SimpleEither<StringPos<NULL>, NULL> name, boolean ifExists) {
        return null;
    }

    @Override
    public NULL dropIndex(NULL p, StringPos<NULL> label, List<StringPos<NULL>> propertyNames) {
        return null;
    }

    @Override
    public NULL createRole(
            NULL p,
            boolean replace,
            SimpleEither<StringPos<NULL>, NULL> roleName,
            SimpleEither<StringPos<NULL>, NULL> fromRole,
            boolean ifNotExists) {
        return null;
    }

    @Override
    public NULL dropRole(NULL p, SimpleEither<StringPos<NULL>, NULL> roleName, boolean ifExists) {
        return null;
    }

    @Override
    public NULL renameRole(
            NULL p,
            SimpleEither<StringPos<NULL>, NULL> fromRoleName,
            SimpleEither<StringPos<NULL>, NULL> toRoleName,
            boolean ifExists) {
        return null;
    }

    @Override
    public NULL showRoles(
            NULL p, boolean withUsers, boolean showAll, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public NULL grantRoles(
            NULL p, List<SimpleEither<StringPos<NULL>, NULL>> roles, List<SimpleEither<StringPos<NULL>, NULL>> users) {
        return null;
    }

    @Override
    public NULL revokeRoles(
            NULL p, List<SimpleEither<StringPos<NULL>, NULL>> roles, List<SimpleEither<StringPos<NULL>, NULL>> users) {
        return null;
    }

    @Override
    public NULL createUser(
            NULL p,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<NULL>, NULL> username,
            Boolean suspended,
            NULL homeDatabase,
            List<NULL> auths,
            List<NULL> systemAuthAttributes) {
        return null;
    }

    @Override
    public NULL dropUser(NULL p, boolean ifExists, SimpleEither<StringPos<NULL>, NULL> username) {
        return null;
    }

    @Override
    public NULL renameUser(
            NULL p,
            SimpleEither<StringPos<NULL>, NULL> fromUserName,
            SimpleEither<StringPos<NULL>, NULL> toUserName,
            boolean ifExists) {
        return null;
    }

    @Override
    public NULL setOwnPassword(NULL p, NULL currentPassword, NULL newPassword) {
        return null;
    }

    @Override
    public NULL alterUser(
            NULL p,
            boolean ifExists,
            SimpleEither<StringPos<NULL>, NULL> username,
            Boolean suspended,
            NULL homeDatabase,
            boolean removeHome,
            List<NULL> auths,
            List<NULL> systemAuthAttributes,
            boolean removeAllAuth,
            List<NULL> removeAuths) {
        return null;
    }

    @Override
    public NULL auth(String provider, List<NULL> nulls, NULL p) {
        return null;
    }

    @Override
    public NULL authId(NULL s, NULL id) {
        return null;
    }

    @Override
    public NULL password(NULL p, NULL password, boolean encrypted) {
        return null;
    }

    @Override
    public NULL passwordChangeRequired(NULL p, boolean changeRequired) {
        return null;
    }

    @Override
    public NULL passwordExpression(NULL password) {
        return null;
    }

    @Override
    public NULL passwordExpression(NULL s, NULL e, String password) {
        return null;
    }

    @Override
    public NULL showUsers(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull, boolean withAuth) {
        return null;
    }

    @Override
    public NULL showCurrentUser(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public NULL showSupportedPrivileges(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public NULL showAllPrivileges(
            NULL p, boolean asCommand, boolean asRevoke, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public NULL showRolePrivileges(
            NULL p,
            List<SimpleEither<StringPos<NULL>, NULL>> roles,
            boolean asCommand,
            boolean asRevoke,
            NULL yieldExpr,
            NULL returnWithoutGraph,
            NULL aNull) {
        return null;
    }

    @Override
    public NULL showUserPrivileges(
            NULL p,
            List<SimpleEither<StringPos<NULL>, NULL>> users,
            boolean asCommand,
            boolean asRevoke,
            NULL yieldExpr,
            NULL returnWithoutGraph,
            NULL aNull) {
        return null;
    }

    @Override
    public NULL grantPrivilege(NULL p, List<SimpleEither<StringPos<NULL>, NULL>> roles, NULL privilege) {
        return null;
    }

    @Override
    public NULL denyPrivilege(NULL p, List<SimpleEither<StringPos<NULL>, NULL>> roles, NULL privilege) {
        return null;
    }

    @Override
    public NULL revokePrivilege(
            NULL p,
            List<SimpleEither<StringPos<NULL>, NULL>> roles,
            NULL privilege,
            boolean revokeGrant,
            boolean revokeDeny) {
        return null;
    }

    @Override
    public NULL databasePrivilege(NULL p, NULL aNull, NULL scope, List<NULL> qualifier, boolean immutable) {
        return null;
    }

    @Override
    public NULL dbmsPrivilege(NULL p, NULL aNull, List<NULL> qualifier, boolean immutable) {
        return null;
    }

    @Override
    public NULL graphPrivilege(NULL p, NULL aNull, NULL scope, NULL aNull2, List<NULL> qualifier, boolean immutable) {
        return null;
    }

    @Override
    public NULL loadPrivilege(
            NULL p, SimpleEither<String, NULL> url, SimpleEither<String, NULL> cidr, boolean immutable) {
        return null;
    }

    @Override
    public NULL privilegeAction(ActionType action) {
        return null;
    }

    @Override
    public NULL propertiesResource(NULL p, List<String> property) {
        return null;
    }

    @Override
    public NULL allPropertiesResource(NULL p) {
        return null;
    }

    @Override
    public NULL labelsResource(NULL p, List<String> label) {
        return null;
    }

    @Override
    public NULL allLabelsResource(NULL p) {
        return null;
    }

    @Override
    public NULL databaseResource(NULL p) {
        return null;
    }

    @Override
    public NULL noResource(NULL p) {
        return null;
    }

    @Override
    public NULL labelQualifier(NULL p, String label) {
        return null;
    }

    @Override
    public NULL allLabelsQualifier(NULL p) {
        return null;
    }

    @Override
    public NULL relationshipQualifier(NULL p, String relationshipType) {
        return null;
    }

    @Override
    public NULL allRelationshipsQualifier(NULL p) {
        return null;
    }

    @Override
    public NULL elementQualifier(NULL p, String name) {
        return null;
    }

    @Override
    public NULL allElementsQualifier(NULL p) {
        return null;
    }

    @Override
    public NULL patternQualifier(List<NULL> qualifiers, NULL variable, NULL expression) {
        return null;
    }

    @Override
    public List<NULL> allQualifier() {
        return null;
    }

    @Override
    public List<NULL> allDatabasesQualifier() {
        return null;
    }

    @Override
    public List<NULL> userQualifier(List<SimpleEither<StringPos<NULL>, NULL>> users) {
        return null;
    }

    @Override
    public List<NULL> allUsersQualifier() {
        return null;
    }

    @Override
    public List<NULL> functionQualifier(NULL p, List<String> functions) {
        return null;
    }

    @Override
    public List<NULL> procedureQualifier(NULL p, List<String> procedures) {
        return null;
    }

    @Override
    public List<NULL> settingQualifier(NULL p, List<String> names) {
        return null;
    }

    @Override
    public NULL graphScope(NULL p, List<NULL> graphNames, ScopeType scopeType) {
        return null;
    }

    @Override
    public NULL databaseScope(NULL p, List<NULL> databaseNames, ScopeType scopeType) {
        return null;
    }

    @Override
    public NULL enableServer(
            NULL p, SimpleEither<String, NULL> serverName, SimpleEither<Map<String, NULL>, NULL> options) {
        return null;
    }

    @Override
    public NULL alterServer(
            NULL p, SimpleEither<String, NULL> serverName, SimpleEither<Map<String, NULL>, NULL> options) {
        return null;
    }

    @Override
    public NULL renameServer(NULL p, SimpleEither<String, NULL> serverName, SimpleEither<String, NULL> newName) {
        return null;
    }

    @Override
    public NULL dropServer(NULL p, SimpleEither<String, NULL> serverName) {
        return null;
    }

    @Override
    public NULL showServers(NULL p, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public NULL deallocateServers(NULL p, boolean dryRun, List<SimpleEither<String, NULL>> serverNames) {
        return null;
    }

    @Override
    public NULL reallocateDatabases(NULL p, boolean dryRun) {
        return null;
    }

    @Override
    public NULL createDatabase(
            NULL p,
            boolean replace,
            NULL databaseName,
            boolean ifNotExists,
            NULL aNull,
            SimpleEither<Map<String, NULL>, NULL> options,
            Integer topologyPrimaries,
            Integer topologySecondaries) {
        return null;
    }

    @Override
    public NULL createCompositeDatabase(
            NULL p,
            boolean replace,
            NULL compositeDatabaseName,
            boolean ifNotExists,
            SimpleEither<Map<String, NULL>, NULL> options,
            NULL wait) {
        return null;
    }

    @Override
    public NULL dropDatabase(
            NULL p, NULL databaseName, boolean ifExists, boolean composite, boolean dumpData, NULL wait) {
        return null;
    }

    @Override
    public NULL alterDatabase(
            NULL p,
            NULL databaseName,
            boolean ifExists,
            AccessType accessType,
            Integer topologyPrimaries,
            Integer topologySecondaries,
            Map<String, NULL> options,
            Set<String> optionsToRemove,
            NULL waitClause) {
        return null;
    }

    @Override
    public NULL showDatabase(NULL p, NULL aNull, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull2) {
        return null;
    }

    @Override
    public NULL startDatabase(NULL p, NULL databaseName, NULL wait) {
        return null;
    }

    @Override
    public NULL stopDatabase(NULL p, NULL databaseName, NULL wait) {
        return null;
    }

    @Override
    public NULL databaseScope(NULL p, NULL databaseName, boolean isDefault, boolean isHome) {
        return null;
    }

    @Override
    public NULL wait(boolean wait, long seconds) {
        return null;
    }

    @Override
    public NULL createLocalDatabaseAlias(
            NULL p,
            boolean replace,
            NULL aliasName,
            NULL targetName,
            boolean ifNotExists,
            SimpleEither<Map<String, NULL>, NULL> properties) {
        return null;
    }

    @Override
    public NULL createRemoteDatabaseAlias(
            NULL p,
            boolean replace,
            NULL aliasName,
            NULL targetName,
            boolean ifNotExists,
            SimpleEither<String, NULL> url,
            SimpleEither<StringPos<NULL>, NULL> username,
            NULL password,
            SimpleEither<Map<String, NULL>, NULL> driverSettings,
            SimpleEither<Map<String, NULL>, NULL> properties) {
        return null;
    }

    @Override
    public NULL alterLocalDatabaseAlias(
            NULL p,
            NULL aliasName,
            NULL targetName,
            boolean ifExists,
            SimpleEither<Map<String, NULL>, NULL> properties) {
        return null;
    }

    @Override
    public NULL alterRemoteDatabaseAlias(
            NULL p,
            NULL aliasName,
            NULL targetName,
            boolean ifExists,
            SimpleEither<String, NULL> url,
            SimpleEither<StringPos<NULL>, NULL> username,
            NULL password,
            SimpleEither<Map<String, NULL>, NULL> driverSettings,
            SimpleEither<Map<String, NULL>, NULL> properties) {
        return null;
    }

    @Override
    public NULL dropAlias(NULL p, NULL aliasName, boolean ifExists) {
        return null;
    }

    @Override
    public NULL showAliases(NULL p, NULL aliasName, NULL yieldExpr, NULL returnWithoutGraph, NULL aNull) {
        return null;
    }

    @Override
    public void addDeprecatedIdentifierUnicodeNotification(NULL p, Character character, String identifier) {
        // nop
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
        return null;
    }

    @Override
    public NULL databaseName(NULL param) {
        return null;
    }
}
