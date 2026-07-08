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
package org.neo4j.gqlstatus;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class GqlHelper {

    public static ErrorGqlStatusObject getGql08N06(ErrorGqlStatusObject cause) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N06)
                .withCause(cause)
                .build();
    }

    public static ErrorGqlStatusObject getGql08N06_22N04(String input, String context, List<String> inputList) {
        return getGql08N06(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.StringParam.context, context)
                .withParam(GqlParams.ListParam.inputList, inputList)
                .build());
    }

    public static ErrorGqlStatusObject getGql08N09(String databaseName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, databaseName)
                .build();
    }

    public static ErrorGqlStatusObject getGql22000_22N11(String input) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N11)
                        .withParam(GqlParams.StringParam.input, input)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22000_22N51(String dbName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N51)
                        .withParam(GqlParams.StringParam.db, dbName)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22003(String value, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withParam(GqlParams.StringParam.value, value)
                .atPosition(offset, line, column)
                .build();
    }

    public static ErrorGqlStatusObject getGql22003_22N03(
            String component, String valueType, Number lower, Number upper, String value) {
        return getGql22003_22N03(component, valueType, String.valueOf(lower), String.valueOf(upper), value);
    }

    public static ErrorGqlStatusObject getGql22003_22N03(
            String component, String valueType, String lower, String upper, String value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                .withParam(GqlParams.StringParam.value, value)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.lower, lower)
                        .withParam(GqlParams.StringParam.upper, upper)
                        .withParam(GqlParams.StringParam.value, value)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22007_22N01(String value, List<String> valueTypeList, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                        .withParam(GqlParams.StringParam.value, value)
                        .withParam(GqlParams.ListParam.valueTypeList, valueTypeList)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22007_22N03(
            String component, String valueType, Number lower, Number upper, Object value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.lower, String.valueOf(lower))
                        .withParam(GqlParams.StringParam.upper, String.valueOf(upper))
                        .withParam(GqlParams.StringParam.value, String.valueOf(value))
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22007_22N12(String input) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N12)
                        .withParam(GqlParams.StringParam.input, input)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22000_22N21(String crs) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N21)
                        .withParam(GqlParams.StringParam.crs, crs)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22000_22N48(String runtime, String cause) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N48)
                        .withParam(GqlParams.StringParam.runtime, runtime)
                        .withParam(GqlParams.StringParam.cause, cause)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N01(String value, List<String> valueTypeList, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                        .withParam(GqlParams.StringParam.value, value)
                        .withParam(GqlParams.ListParam.valueTypeList, valueTypeList)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N27(String input, String context, List<String> validTypes) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.context, context)
                        .withParam(GqlParams.ListParam.valueTypeList, validTypes)
                        .withParam(GqlParams.StringParam.hint, "")
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N27(
            String input, String context, List<String> validTypes, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.context, context)
                        .withParam(GqlParams.ListParam.valueTypeList, validTypes)
                        .withParam(GqlParams.StringParam.hint, "")
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N27WithHint(
            String input, String context, List<String> validTypes, String hint, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N27)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.context, context)
                        .withParam(GqlParams.ListParam.valueTypeList, validTypes)
                        .withParam(GqlParams.StringParam.hint, hint)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N31(
            String entityType, String propKey, String pattern, String value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N31)
                        .withParam(GqlParams.StringParam.entityType, entityType)
                        .withParam(GqlParams.StringParam.propKey, propKey)
                        .withParam(GqlParams.StringParam.pat, pattern)
                        .withParam(GqlParams.StringParam.value, value)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G03_22N39(String value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N39)
                        .withParam(GqlParams.StringParam.value, value)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G05_22N15(String component, String temporal) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G05)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N15)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.temporal, temporal)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G05_22N40(String component, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G05)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N40)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G06_22N40(String component, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G06)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N40)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22G08_22N40(String component, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G08)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N40)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22007_22N25(String valueType, String temporal) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N25)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.temporal, temporal)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22007_22N36(String input, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N36)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get22015_22N28(String operation) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22015)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N28)
                        .withParam(GqlParams.StringParam.operation, operation)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22N05_22N84(String input, String context, int upper) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.StringParam.context, context)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N84)
                        .withParam(GqlParams.NumberParam.upper, upper)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22N38_22N01(
            String functionName, String value, List<String> valueTypeList, String valueType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N38)
                .withParam(GqlParams.StringParam.value, functionName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                        .withParam(GqlParams.StringParam.value, value)
                        .withParam(GqlParams.ListParam.valueTypeList, valueTypeList)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22N38_22N03(
            String fun, String component, String valueType, Number lower, Number upper, Number value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N38)
                .withParam(GqlParams.StringParam.value, GqlParams.StringParam.fun.process(fun))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                        .withParam(GqlParams.StringParam.component, component)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.lower, String.valueOf(lower))
                        .withParam(GqlParams.StringParam.upper, String.valueOf(upper))
                        .withParam(GqlParams.StringParam.value, String.valueOf(value))
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22N38_22N04(
            String fun, String input, String context, List<String> inputList) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N38)
                .withParam(GqlParams.StringParam.value, GqlParams.StringParam.fun.process(fun))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.context, context)
                        .withParam(GqlParams.ListParam.inputList, inputList)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22N38_22NB1(
            String function, List<String> expectedTypeList, String actualType) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N38)
                .withParam(GqlParams.StringParam.value, function)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB1)
                        .withParam(GqlParams.ListParam.valueTypeList, expectedTypeList)
                        .withParam(GqlParams.StringParam.input, actualType)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NB1(
            List<String> expectedTypeList, String actualType, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB1)
                        .withParam(GqlParams.ListParam.valueTypeList, expectedTypeList)
                        .withParam(GqlParams.StringParam.input, actualType)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get22N69_52N02(String idxDescrOrName, String proc) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N69)
                .withParam(GqlParams.StringParam.idxDescrOrName, idxDescrOrName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                        .withParam(GqlParams.StringParam.proc, proc)
                        .build())
                .build();
    }

    private static ErrorGqlStatusObject getGql22N77(
            String entityType, long entityId, String tokenType, String token, String[] propKeyList) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N77)
                .withParam(GqlParams.StringParam.entityType, entityType)
                .withParam(GqlParams.NumberParam.entityId, entityId)
                .withParam(GqlParams.StringParam.tokenType, tokenType)
                .withParam(GqlParams.StringParam.token, token)
                .withParam(GqlParams.ListParam.propKeyList, List.of(propKeyList))
                .build();
    }

    public static ErrorGqlStatusObject getGql22N77_nodes(long nodeId, String token, String[] propKeyList) {
        return getGql22N77("NODE", nodeId, "label", token, propKeyList);
    }

    public static ErrorGqlStatusObject getGql22N77_relationships(
            long relationshipId, String token, String[] propKeyList) {
        return getGql22N77("RELATIONSHIP", relationshipId, "type", token, propKeyList);
    }

    public static ErrorGqlStatusObject getGql22N81(String exprType, String context, int offset, int line, int column) {
        var builder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N81)
                .withParam(GqlParams.StringParam.exprType, exprType)
                .withParam(GqlParams.StringParam.context, context);
        if (offset >= 0) {
            builder.atPosition(offset, line, column);
        }
        return builder.build();
    }

    public static ErrorGqlStatusObject getGql22N81(String exprType, String context) {
        return getGql22N81(exprType, context, -1, -1, -1);
    }

    public static ErrorGqlStatusObject getGql22NA0_22NA4(String predicate) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA4)
                        .withParam(GqlParams.StringParam.pred, predicate)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22NA0_22NA5(String predicate) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA5)
                        .withParam(GqlParams.StringParam.pred, predicate)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22NA0_22NA6(String predicate) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA6)
                        .withParam(GqlParams.StringParam.pred, predicate)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22NA0_22NB0(String predicate) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB0)
                        .withParam(GqlParams.StringParam.pred, predicate)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql22ND3(String propertyName, String indexName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22ND3)
                .withParam(GqlParams.StringParam.propKey, propertyName)
                .withParam(GqlParams.StringParam.idx, indexName)
                .build();
    }

    public static ErrorGqlStatusObject getGql42N45(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N45)
                .atPosition(offset, line, column)
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_withCause(
            ErrorGqlStatusObjectImplementation.Builder causeBuilder, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(causeBuilder.atPosition(offset, line, column).build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22N04(
            String input, String context, List<?> validTypes, int offset, int line, int column) {
        ErrorGqlStatusObjectImplementation.Builder main = ErrorGqlStatusObjectImplementation.from(
                        GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column);
        ErrorGqlStatusObjectImplementation.Builder cause = ErrorGqlStatusObjectImplementation.from(
                        GqlStatusInfoCodes.STATUS_22N04)
                .atPosition(offset, line, column)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.StringParam.context, context)
                .withParam(GqlParams.ListParam.inputList, validTypes);
        if (offset >= 0) {
            main.atPosition(offset, line, column);
            cause.atPosition(offset, line, column);
        }
        return main.withCause(cause.build()).build();
    }

    public static ErrorGqlStatusObject getGql42001_22N04(String input, String variable, List<?> validTypes) {
        return getGql42001_22N04(input, variable, validTypes, -1, -1, -1);
    }

    public static ErrorGqlStatusObject getGql42001_22N65(String constraintDescr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N65)
                        .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescr)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22N66(String constraintDescr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N66)
                        .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescr)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NC4(List<String> labelList, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC4)
                        .withParam(GqlParams.ListParam.labelList, labelList)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NC5(
            String graphTypeElement, String entityType, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC5)
                        .withParam(GqlParams.StringParam.entityType, entityType)
                        .withParam(GqlParams.StringParam.graphTypeReference, graphTypeElement)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NC6(
            String constraintDescriptionOrName, String label, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC6)
                        .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescriptionOrName)
                        .withParam(GqlParams.StringParam.label, label)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NC7(
            String constraintDescriptionOrName, String relationshipType, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC7)
                        .withParam(GqlParams.StringParam.constrDescrOrName, constraintDescriptionOrName)
                        .withParam(GqlParams.StringParam.relType, relationshipType)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_22NC9(
            String context, String entityType, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC9)
                        .withParam(GqlParams.StringParam.entityType, entityType)
                        .withParam(GqlParams.StringParam.context, context)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I04(String expr, String clause, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I04)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.expr, expr)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I80(
            String element, String alias, boolean referencesAggregation, int offset, int line, int column) {
        String reason = referencesAggregation
                ? "A grouping element cannot reference the aggregation `" + alias + "`."
                : "A grouping element that references the projection item alias `" + alias
                        + "` must be a simple variable reference.";
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I80)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.expr, element)
                        .withParam(GqlParams.StringParam.cause, reason)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I06(
            String input, List<String> expectedList, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I06)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.ListParam.valueList, expectedList)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I07(
            String valueType, String input, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I07)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.input, input)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I10(String syntax, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I10)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.syntax, syntax)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I11(
            String tokenType, String input, int offset, int line, int column) {
        String inputChecked = (input == null) ? "Null" : input;
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I11)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.tokenType, tokenType)
                        .withParam(GqlParams.StringParam.input, inputChecked)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I13(
            int expectedNumberOfArgs,
            int obtainedNumberOfArgs,
            String procedureFunction,
            String signature,
            int offset,
            int line,
            int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.NumberParam.count1, expectedNumberOfArgs)
                        .withParam(GqlParams.NumberParam.count2, obtainedNumberOfArgs)
                        .withParam(GqlParams.StringParam.procFun, procedureFunction)
                        .withParam(GqlParams.StringParam.sig, signature)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I14(String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I14)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I15(int size, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I15)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.NumberParam.count, size)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I20(
            String input, String labelExpr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I20)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.labelExpr, labelExpr)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I21(
            List<String> invalidReferences, String stringifiedPattern, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I21)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.ListParam.variableList, invalidReferences)
                        .withParam(GqlParams.StringParam.pat, stringifiedPattern)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I23(String funcName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I23)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.fun, funcName)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I24(String expression, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I24)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.expr, expression)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I25(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I25)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I28(String input, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I28)
                        .withParam(GqlParams.StringParam.input, input)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I29(
            String input, String replacement, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I29)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.input, input)
                        .withParam(GqlParams.StringParam.replacement, replacement)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I30(String expr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I30)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.expr, expr)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I32(String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I32)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I35(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I35)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I36(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I36)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I39(String fun, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I39)
                        .withParam(GqlParams.StringParam.fun, fun)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I42(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I42)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I45(String action, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I45)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.action, action)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I46(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I46)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I47(String msg, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I47)
                        .withParam(GqlParams.StringParam.msg, msg)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I48(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I48)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I52(String message, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I52)
                        .withParam(GqlParams.StringParam.msg, message)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I58(String entity, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I58)
                        .withParam(GqlParams.StringParam.expr, entity)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I69(String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I69)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I70(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I70)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I71(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I71)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I72(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I72)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I73(String expr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I73)
                        .withParam(GqlParams.StringParam.expr, expr)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I74(
            String variable1, String variable2, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I74)
                        .withParam(GqlParams.StringParam.variable1, variable1)
                        .withParam(GqlParams.StringParam.variable2, variable2)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I75(
            String expression, String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I75)
                        .withParam(GqlParams.StringParam.expr, expression)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42I77(String name, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I77)
                        .withParam(GqlParams.StringParam.proc, name)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N07(String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N07)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N0A(String action, String db1, String db2) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N0A)
                        .withParam(GqlParams.StringParam.action, action)
                        .withParam(GqlParams.StringParam.db1, db1)
                        .withParam(GqlParams.StringParam.db2, db2)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N14(String clause, String cmd, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N14)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .withParam(GqlParams.StringParam.cmd, cmd)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N14_WithoutPosition(String clause, String cmd) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N14)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .withParam(GqlParams.StringParam.cmd, cmd)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N19(String clause, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N19)
                        .withParam(GqlParams.StringParam.syntax, clause)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N21(String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N21)
                        .withParam(GqlParams.StringParam.context, context)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N22(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N22)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N23(String clause, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N23)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N24(
            String input1, String input2, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N24)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.input1, input1)
                        .withParam(GqlParams.StringParam.input2, input2)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N25(
            List<String> availableColumns, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N25)
                        .withParam(GqlParams.ListParam.variableList, availableColumns)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N28(String name, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N28)
                        .withParam(GqlParams.StringParam.input, name)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N32(String keyword, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N32)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.keyword, keyword)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N34(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N34)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N36(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N36)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N37(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N37)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N38(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N38)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N39(String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N39)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N3A(String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N3A)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N3B(String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N3B)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.context, context)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N3C(String clause, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N3C)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N40(String funcName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N40)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.fun, funcName)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N42(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N42)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N44(
            String variable, String clause, boolean groupBySupported, int offset, int line, int column) {
        String groupingConstructs = groupBySupported
                ? "`DISTINCT`, an aggregation, or a `GROUP BY` clause"
                : "`DISTINCT` or an aggregation";
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N44)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .withParam(GqlParams.StringParam.clause, clause)
                        .withParam(GqlParams.StringParam.groupingConstructs, groupingConstructs)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N45(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(getGql42N45(offset, line, column))
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N47(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N47)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N48(String functionName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N48)
                        .withParam(GqlParams.StringParam.fun, functionName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N08(String procedureName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, procedureName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N50(
            String returnColumn, List<String> availableColumns, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N50)
                        .withParam(GqlParams.StringParam.arg, returnColumn)
                        .withParam(GqlParams.ListParam.argList, availableColumns)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N53(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N53)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N54(String matchMode, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N54)
                        .withParam(GqlParams.StringParam.matchMode, matchMode)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_51N26(
            String item, String feature, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N26)
                        .withParam(GqlParams.StringParam.item, item)
                        .withParam(GqlParams.StringParam.feat, feature)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N57(String expr, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N57)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.expr, expr)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N58(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N58)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N59(String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N59)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N60(String pathMode, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N60)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.pathMode, pathMode)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N61(List<String> pathModes, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N61)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.ListParam.pathModes, pathModes)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N62(String variable, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N62)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N63(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N63)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N64(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N64)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N67(String parameter, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N67)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.procParam, parameter)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N69(
            String shortestPathFunc, String expression, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N69)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.fun, shortestPathFunc)
                        .withParam(GqlParams.StringParam.expr, expression)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N71(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N71)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N71_42NAB(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N71)
                        .atPosition(offset, line, column)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAB)
                                .atPosition(offset, line, column)
                                .build())
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N72(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N72)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N73(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N73)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N74(int offset, int line, int column, String db1, String db2) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N74)
                        .atPosition(offset, line, column)
                        .withParam(GqlParams.StringParam.db1, db1)
                        .withParam(GqlParams.StringParam.db2, db2)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N7A(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N7A)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N7B(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N7B)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N7C(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .atPosition(offset, line, column)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N7C)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N00(String db) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N00)
                        .withParam(GqlParams.StringParam.db, db)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42N81(List<String> expectedParameters, List<String> gotParameters) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N81)
                        .withParam(GqlParams.ListParam.paramList1, expectedParameters)
                        .withParam(GqlParams.ListParam.paramList2, gotParameters)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get42N00_databaseNotFound(String command, String db, String paramName) {
        var invalidDbCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N00)
                .withParam(GqlParams.StringParam.db, db);

        return invalidSyntax()
                .withCause(invalidReference(command)
                        .withCause(
                                maybeInvalidParameter(paramName, invalidDbCause).build())
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get42N09_userNotFound(String user) {
        var invalidUserCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N09)
                .withParam(GqlParams.StringParam.user, user)
                .build();

        return invalidSyntax().withCause(invalidUserCause).build();
    }

    public static ErrorGqlStatusObject get42N09_userNotFound(String command, String user, String paramName) {
        var invalidUserCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N09)
                .withParam(GqlParams.StringParam.user, user);

        return invalidSyntax()
                .withCause(invalidReference(command)
                        .withCause(maybeInvalidParameter(paramName, invalidUserCause)
                                .build())
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get42N10_roleNotFound(String command, String role, String paramName) {
        var invalidRoleCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N10)
                .withParam(GqlParams.StringParam.role, role);

        return invalidSyntax()
                .withCause(invalidReference(command)
                        .withCause(maybeInvalidParameter(paramName, invalidRoleCause)
                                .build())
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get42NAD_authRuleNotFound(String command, String role, String paramName) {
        var invalidRoleCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAD)
                .withParam(GqlParams.StringParam.authRule, role);

        return invalidSyntax()
                .withCause(invalidReference(command)
                        .withCause(maybeInvalidParameter(paramName, invalidRoleCause)
                                .build())
                        .build())
                .build();
    }

    public static ErrorGqlStatusObjectImplementation.Builder invalidSyntax() {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001);
    }

    private static ErrorGqlStatusObjectImplementation.Builder invalidReference(String command) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NA8)
                .withParam(GqlParams.StringParam.cmd, command);
    }

    private static ErrorGqlStatusObjectImplementation.Builder invalidParameter(String paramName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N51)
                .withParam(GqlParams.StringParam.param, paramName);
    }

    public static ErrorGqlStatusObjectImplementation.Builder maybeInvalidParameter(
            String paramName, ErrorGqlStatusObjectImplementation.Builder cause) {
        if (paramName != null) {
            return invalidParameter(paramName).withCause(cause.build());
        } else {
            return cause;
        }
    }

    public static ErrorGqlStatusObject getGql42001_42NAF() {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAF)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAG() {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAG)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAH(
            String columnName, String procedureName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAH)
                        .withParam(GqlParams.StringParam.ident, columnName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAI(
            String columnName, String procedureName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAI)
                        .withParam(GqlParams.StringParam.ident, columnName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAJ(
            String typeName, String columnName, String procedureName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAJ)
                        .withParam(GqlParams.StringParam.typeDescription, typeName)
                        .withParam(GqlParams.StringParam.ident, columnName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    // reserved for local functions
    public static ErrorGqlStatusObject getGql42001_42NAK(
            String typeName, String functionName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAK)
                        .withParam(GqlParams.StringParam.typeDescription, typeName)
                        .withParam(GqlParams.StringParam.fun, functionName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAL(
            String typeName, String columnName, String procedureName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAL)
                        .withParam(GqlParams.StringParam.typeDescription, typeName)
                        .withParam(GqlParams.StringParam.ident, columnName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAN(String functionName, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAN)
                        .withParam(GqlParams.StringParam.fun, functionName)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42001_42NAO(int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NAO)
                        .atPosition(offset, line, column)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject getGql42N51(String parameter, ErrorGqlStatusObject cause) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N51)
                .withParam(GqlParams.StringParam.param, parameter)
                .withCause(cause)
                .build();
    }

    public static ErrorGqlStatusObject get50N00(String msgTitle, String msg) {
        return get50N00(msgTitle, msg, -1, -1, -1);
    }

    public static ErrorGqlStatusObject get50N00(String msgTitle, String msg, int offset, int line, int column) {
        var builder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, msgTitle)
                .withParam(GqlParams.StringParam.msg, msg);
        if (offset >= 0) builder.atPosition(offset, line, column);
        return builder.build();
    }

    public static ErrorGqlStatusObject get50N22(String database) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N22)
                .withParam(GqlParams.StringParam.db, database)
                .build();
    }

    public static ErrorGqlStatusObject get50N23(int retries, double timeoutInSeconds) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N23)
                .withParam(GqlParams.NumberParam.count, retries)
                .withParam(GqlParams.NumberParam.timeAmount, timeoutInSeconds)
                .withParam(
                        GqlParams.StringParam.timeUnit,
                        TimeUnit.SECONDS.toString().toLowerCase(Locale.ROOT))
                .build();
    }

    public static ErrorGqlStatusObject getGql51N00_51N18(String procMethod) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N18)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get51N00_52N35(String procClass, String msg) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N35)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.msg, msg)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get51N00_53N35(String funClass, String msg) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53N35)
                        .withParam(GqlParams.StringParam.funClass, funClass)
                        .withParam(GqlParams.StringParam.msg, msg)
                        .build())
                .build();
    }

    public static ErrorGqlStatusObject get51N31(String notSupported, String context, int offset, int line, int column) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N31)
                .atPosition(offset, line, column)
                .withParam(GqlParams.StringParam.feat, notSupported)
                .withParam(GqlParams.StringParam.context, context)
                .build();
    }

    public static ErrorGqlStatusObject get51N31(String notSupported, String context) {
        return get51N31(notSupported, context, -1, -1, -1);
    }

    public static ErrorGqlStatusObject get52N34(String proc) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N34)
                .withParam(GqlParams.StringParam.proc, proc)
                .build();
    }

    public static ErrorGqlStatusObject get52N37(String procedureName, ErrorGqlStatusObject cause) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N37)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withCause(cause)
                .build();
    }

    public static ErrorGqlStatusObject get52U00(String procedureName, Throwable e) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52U00)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withParam(GqlParams.StringParam.msgTitle, e.getClass().getName())
                .withParam(GqlParams.StringParam.msg, e.getMessage())
                .build();
    }

    public static ErrorGqlStatusObject get53N34(String funcName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53N34)
                .withParam(GqlParams.StringParam.fun, funcName)
                .build();
    }

    public static ErrorGqlStatusObject get53N37(String funcName, ErrorGqlStatusObject cause) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53N37)
                .withParam(GqlParams.StringParam.fun, funcName)
                .withCause(cause)
                .build();
    }

    public static ErrorGqlStatusObject get53U00(String funcName, Throwable e) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53U00)
                .withParam(GqlParams.StringParam.fun, funcName)
                .withParam(GqlParams.StringParam.msgTitle, e.getClass().getName())
                .withParam(GqlParams.StringParam.msg, e.getMessage())
                .build();
    }

    public static ErrorGqlStatusObject get22G0I(String field) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G0I)
                .withParam(GqlParams.StringParam.field, field)
                .build();
    }

    public static ErrorGqlStatusObject get22NAC(String input, Long position, String value) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAC)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.NumberParam.pos, position)
                .withParam(GqlParams.StringParam.variable, value)
                .build();
    }

    public static ErrorGqlStatusObject get22NAD(String input, Long position) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAD)
                .withParam(GqlParams.NumberParam.pos, position)
                .withParam(GqlParams.StringParam.input, input)
                .build();
    }

    public static ErrorGqlStatusObject get22NAE(String input, Long position) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NAE)
                .withParam(GqlParams.StringParam.input, input)
                .withParam(GqlParams.NumberParam.pos, position)
                .build();
    }

    public static ErrorGqlStatusObject getDefaultObject() {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                .build();
    }

    public static boolean causeChainContains(ErrorGqlStatusObject gso, GqlStatusInfoCodes gqlStatusCode) {
        return gso.gqlStatus().equals(gqlStatusCode.getStatusString())
                || (gso.cause().isPresent() && causeChainContains(gso.cause().get(), gqlStatusCode));
    }

    /**
     * Append the exception cause as the bottom GQL cause of the inner ErrorGqlStatusObject if the following applies
     * - the exception cause is an ErrorGqlStatusObject (and not e.g. a generic Java exception)
     * - the inner ErrorGqlStatusObject is of type ErrorGqlStatusObjectImplementation
     * (this should always be true, but is needed for casting)
     *
     * @param gqlStatusObject The current inner ErrorGqlStatusObject
     * @param cause           The exception cause
     * @return The replaced inner ErrorGqlStatusObject
     */
    public static ErrorGqlStatusObject getInnerGqlStatusObject(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        if (cause instanceof ErrorGqlStatusObject gqlStatusObjectCause) {
            // There are exceptions which are wrappers on top of other exceptions for being compliant with checked
            // exceptions and re-thrown.
            // On those scenarios, the original gqlStatusObject is copied to the new exception and
            // the wrapped exception is added as a cause.
            // So, we don't need to put this gqlStatusObject on the chain since it is already the same object.
            if (gqlStatusObject == gqlStatusObjectCause) {
                return gqlStatusObject;
            }
            Set<ErrorGqlStatusObject> observedStatuses = Collections.newSetFromMap(new IdentityHashMap<>());
            observedStatuses.add(gqlStatusObject);
            return getErrorObjectWithRewrittenCause(gqlStatusObject, gqlStatusObjectCause, observedStatuses);
        } else {
            return gqlStatusObject;
        }
    }

    /**
     * Generate a message based on the message of the ErrorGqlStatusObject and appending the messages from the causes.
     *
     * @param gqlStatusObject The ErrorGqlStatusObject
     * @return The message
     */
    public static String getCompleteMessage(ErrorGqlStatusObject gqlStatusObject) {
        if (gqlStatusObject.cause().isPresent()) {
            return gqlStatusObject.getMessage()
                    + System.lineSeparator()
                    + getCompleteMessage(gqlStatusObject.cause().get());
        } else {
            return gqlStatusObject.getMessage();
        }
    }

    private static ErrorGqlStatusObject getErrorObjectWithRewrittenCause(
            ErrorGqlStatusObject gqlStatusObject,
            ErrorGqlStatusObject exceptionCause,
            Set<ErrorGqlStatusObject> observedStatuses) {
        // This should always be true, but needed for casting
        if (gqlStatusObject.gqlStatusObject() instanceof ErrorGqlStatusObjectImplementation gsoImplementation) {
            observedStatuses.add(gsoImplementation);
            ErrorGqlStatusObject newCause;
            if (gqlStatusObject.cause().isPresent()) {
                var currentCause = gqlStatusObject.cause().get();
                if (currentCause.equals(exceptionCause)) {
                    return gqlStatusObject;
                }
                newCause = getErrorObjectWithRewrittenCause(currentCause, exceptionCause, observedStatuses);
            } else {
                // Bottom of the current cause chain => add the Java cause as a GQL cause here
                if (exceptionCause.gqlStatusObject() != null) {
                    /*
                     * The Java cause is an exception implementing ErrorGqlStatusObject
                     * and is having an inner error object
                     * => add the inner error object as GQL cause
                     */
                    newCause = exceptionCause.gqlStatusObject();
                } else {
                    /*
                     * The Java cause is an exception implementing ErrorGqlStatusObject
                     * but is not is having an inner error object
                     * => the cause was not ported to the new framework yet
                     * => add the default error object as GQL cause if it is not already present
                     */
                    if (gqlStatusObject.gqlStatus().equals(ErrorGqlStatusObject.DEFAULT_STATUS_CODE)) {
                        return gqlStatusObject;
                    }
                    newCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42)
                            .build();
                }
            }
            if (observedStatuses.add(newCause)) {
                gsoImplementation.setCause(newCause);
            }
            return gsoImplementation;
        }
        return gqlStatusObject;
    }
}
