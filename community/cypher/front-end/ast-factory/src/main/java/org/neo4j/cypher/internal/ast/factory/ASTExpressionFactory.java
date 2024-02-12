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
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;
import org.neo4j.cypher.internal.ast.factory.ASTFactory.StringPos;

/**
 * Factory for constructing AST expressions.
 * <p>
 * This interface is generic in many dimensions, in order to support type-safe construction of ASTs
 * without depending on the concrete AST type. This architecture allows code which creates/manipulates AST
 * to live independently of the AST, and thus makes sharing and reuse of these components much easier.
 * <p>
 * The factory contains methods for creating AST representing all of Cypher 9 expressions, as defined
 * at `https://github.com/opencypher/openCypher/`, and implemented in `https://github.com/opencypher/front-end`.
 */
public interface ASTExpressionFactory<
        EXPRESSION,
        LABEL_EXPRESSION,
        PARAMETER,
        PATTERN,
        QUERY,
        WHERE,
        VARIABLE extends EXPRESSION,
        PROPERTY extends EXPRESSION,
        FUNCTION_INVOCATION extends EXPRESSION,
        MAP_PROJECTION_ITEM,
        POS,
        ENTITY_TYPE,
        MATCH_MODE> {
    VARIABLE newVariable(POS p, String name);

    PARAMETER newParameter(POS p, VARIABLE v, ParameterType type);

    PARAMETER newParameter(POS p, String offset, ParameterType type);

    PARAMETER newSensitiveStringParameter(POS p, VARIABLE v);

    PARAMETER newSensitiveStringParameter(POS p, String offset);

    EXPRESSION newDouble(POS p, String image);

    EXPRESSION newDecimalInteger(POS p, String image, boolean negated);

    EXPRESSION newHexInteger(POS p, String image, boolean negated);

    EXPRESSION newOctalInteger(POS p, String image, boolean negated);

    EXPRESSION newString(POS start, POS end, String image);

    EXPRESSION newTrueLiteral(POS p);

    EXPRESSION newFalseLiteral(POS p);

    EXPRESSION newInfinityLiteral(POS p);

    EXPRESSION newNaNLiteral(POS p);

    EXPRESSION newNullLiteral(POS p);

    EXPRESSION listLiteral(POS p, List<EXPRESSION> values);

    EXPRESSION mapLiteral(POS p, List<StringPos<POS>> keys, List<EXPRESSION> values);

    PROPERTY property(EXPRESSION subject, StringPos<POS> propertyKeyName);

    EXPRESSION or(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION xor(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION and(POS p, EXPRESSION lhs, EXPRESSION rhs);

    LABEL_EXPRESSION labelConjunction(POS p, LABEL_EXPRESSION lhs, LABEL_EXPRESSION rhs, boolean containsIs);

    LABEL_EXPRESSION labelDisjunction(POS p, LABEL_EXPRESSION lhs, LABEL_EXPRESSION rhs, boolean containsIs);

    LABEL_EXPRESSION labelNegation(POS p, LABEL_EXPRESSION e, boolean containsIs);

    LABEL_EXPRESSION labelWildcard(POS p, boolean containsIs);

    LABEL_EXPRESSION labelLeaf(POS p, String e, ENTITY_TYPE entityType, boolean containsIs);

    LABEL_EXPRESSION labelColonConjunction(POS p, LABEL_EXPRESSION lhs, LABEL_EXPRESSION rhs, boolean containsIs);

    LABEL_EXPRESSION labelColonDisjunction(POS p, LABEL_EXPRESSION lhs, LABEL_EXPRESSION rhs, boolean containsIs);

    EXPRESSION labelExpressionPredicate(EXPRESSION subject, LABEL_EXPRESSION exp);

    EXPRESSION ands(List<EXPRESSION> exprs);

    EXPRESSION not(POS p, EXPRESSION e);

    EXPRESSION plus(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION minus(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION multiply(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION divide(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION modulo(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION pow(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION unaryPlus(EXPRESSION e);

    EXPRESSION unaryPlus(POS p, EXPRESSION e);

    EXPRESSION unaryMinus(POS p, EXPRESSION e);

    EXPRESSION eq(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION neq(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION neq2(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION lte(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION gte(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION lt(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION gt(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION regeq(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION startsWith(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION endsWith(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION contains(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION in(POS p, EXPRESSION lhs, EXPRESSION rhs);

    EXPRESSION isNull(POS p, EXPRESSION e);

    EXPRESSION isNotNull(POS p, EXPRESSION e);

    EXPRESSION isTyped(POS p, EXPRESSION e, ParserCypherTypeName typeName);

    EXPRESSION isNotTyped(POS p, EXPRESSION e, ParserCypherTypeName typeName);

    EXPRESSION isNormalized(POS p, EXPRESSION e, ParserNormalForm normalForm);

    EXPRESSION isNotNormalized(POS p, EXPRESSION e, ParserNormalForm normalForm);

    EXPRESSION listLookup(EXPRESSION list, EXPRESSION index);

    EXPRESSION listSlice(POS p, EXPRESSION list, EXPRESSION start, EXPRESSION end);

    EXPRESSION newCountStar(POS p);

    FUNCTION_INVOCATION functionInvocation(
            POS p,
            POS functionNamePosition,
            List<String> namespace,
            String name,
            boolean distinct,
            List<EXPRESSION> arguments,
            boolean calledFromUseClause);

    EXPRESSION listComprehension(POS p, VARIABLE v, EXPRESSION list, EXPRESSION where, EXPRESSION projection);

    EXPRESSION patternComprehension(
            POS p,
            POS relationshipPatternPosition,
            VARIABLE v,
            PATTERN pattern,
            EXPRESSION where,
            EXPRESSION projection);

    EXPRESSION reduceExpression(
            POS p, VARIABLE acc, EXPRESSION accExpr, VARIABLE v, EXPRESSION list, EXPRESSION innerExpr);

    EXPRESSION allExpression(POS p, VARIABLE v, EXPRESSION list, EXPRESSION where);

    EXPRESSION anyExpression(POS p, VARIABLE v, EXPRESSION list, EXPRESSION where);

    EXPRESSION noneExpression(POS p, VARIABLE v, EXPRESSION list, EXPRESSION where);

    EXPRESSION singleExpression(POS p, VARIABLE v, EXPRESSION list, EXPRESSION where);

    EXPRESSION normalizeExpression(POS p, EXPRESSION i, ParserNormalForm normalForm);

    EXPRESSION patternExpression(POS p, PATTERN pattern);

    EXPRESSION existsExpression(POS p, MATCH_MODE matchMode, List<PATTERN> patterns, QUERY q, WHERE where);

    EXPRESSION countExpression(POS p, MATCH_MODE matchMode, List<PATTERN> patterns, QUERY q, WHERE where);

    EXPRESSION collectExpression(POS p, QUERY q);

    EXPRESSION mapProjection(POS p, VARIABLE v, List<MAP_PROJECTION_ITEM> items);

    MAP_PROJECTION_ITEM mapProjectionLiteralEntry(StringPos<POS> property, EXPRESSION value);

    MAP_PROJECTION_ITEM mapProjectionProperty(StringPos<POS> property);

    MAP_PROJECTION_ITEM mapProjectionVariable(VARIABLE v);

    MAP_PROJECTION_ITEM mapProjectionAll(POS p);

    EXPRESSION caseExpression(POS p, EXPRESSION e, List<EXPRESSION> whens, List<EXPRESSION> thens, EXPRESSION elze);

    POS inputPosition(int offset, int line, int column);

    ENTITY_TYPE nodeType();

    ENTITY_TYPE relationshipType();

    ENTITY_TYPE nodeOrRelationshipType();
}
