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
package org.neo4j.exceptions;

import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class CypherTypeException extends Neo4jException {

    @Deprecated
    public CypherTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CypherTypeException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    @Deprecated
    public CypherTypeException(String message) {
        super(message);
    }

    public CypherTypeException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static CypherTypeException invalidType(
            String value, List<String> expectedTypes, String actualType, String signature) {
        return new CypherTypeException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                                .withParam(GqlParams.StringParam.value, value)
                                .withParam(GqlParams.ListParam.valueTypeList, expectedTypes)
                                .withParam(GqlParams.StringParam.valueType, actualType)
                                .build())
                        .build(),
                String.format("Wrong type. Expected %s, got %s", signature, actualType));
    }

    public static CypherTypeException nodeCreationNotAMap(String value, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(value, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Parameter provided for node creation is not a Map, instead got %s", value));
    }

    public static CypherTypeException expectedMap(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a map but was %s", got));
    }

    public static CypherTypeException expectedExpressionToBeMap(
            String expression, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a map, but it was :`%s`", expression, got));
    }

    public static CypherTypeException expectedPathButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("PATH"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected path but got %s", gotType));
    }

    public static CypherTypeException expectedNodeRelPath(String gotPretty, String gotClass, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP", "PATH"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected a Node, Relationship or Path, but got a %s", gotClass));
    }

    public static CypherTypeException expectedNodeRelWas(
            String got, String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a Node or Relationship, but it was a %s", got, gotClass));
    }

    public static CypherTypeException expectedNodeRel(String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Node or Relationship, but got a %s", gotClass));
    }

    public static CypherTypeException expectedListValue(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected ListValue but got %s", got));
    }

    public static CypherTypeException expectedList(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected list, got %s", got));
    }

    public static CypherTypeException expectedListFound(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected list but found: %s", got));
    }

    public static CypherTypeException expectedCollection(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a collection, got `$x`", got));
    }

    public static CypherTypeException expectedCollectionWasNot(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected the value for %s to be a collection but it was not.", got));
    }

    public static CypherTypeException planExpectedNode(String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, "Created a plan that uses non-nodes when expecting a node");
    }

    public static CypherTypeException notBool(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("%s is not a boolean value", got));
    }

    public static CypherTypeException cantTreatAsBool(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat that as a boolean: %s", got));
    }

    public static CypherTypeException notNode(String got, String gotType) {
        var gql = GqlHelper.getGql22G03_22N01(got, List.of("NODE"), gotType);
        return new CypherTypeException(gql, String.format("%s is not a node", got));
    }

    public static CypherTypeException expectedString(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("STRING"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedStringNotNull(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("STRING NOT NULL"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedStringOrListOfStringsNotNull(
            String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(
                gotPretty, List.of("STRING NOT NULL", "LIST<STRING NOT NULL>"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedNumber(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedNumericGotNull(String target) {
        var gql = GqlHelper.getGql22G03_22N01("NULL", List.of("INTEGER", "FLOAT"), "NULL");
        return new CypherTypeException(gql, String.format("Expected a numeric value for %s, but got null", target));
    }

    public static CypherTypeException expectedNumericGot(
            String target, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a numeric value for %s, but got: %s", target, got));
    }

    public static CypherTypeException expectedVirtualNode(String gotPretty, String gotTypeName, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected VirtualNodeValue got %s", gotTypeName));
    }

    public static CypherTypeException expectedNodeValue(String gotPretty, String gotTypeName, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected NodeValue but got %s", gotTypeName));
    }

    public static CypherTypeException typeMismatchExpectedANode(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a node but was %s", got));
    }

    public static CypherTypeException typeMismatchExpectedANodeWasType(
            String got, String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Type mismatch: expected a node but was %s of type %s", got, gotClass));
    }

    public static CypherTypeException expectedNode(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Node, got: %s", got));
    }

    public static CypherTypeException expectedNodeButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected node but got %s", gotType));
    }

    public static CypherTypeException expectedANodeButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a node, but got %s ", gotType));
    }

    public static CypherTypeException expectedNodeButGotNull() {
        var gql = GqlHelper.getGql22G03_22N01("NULL", List.of("NODE"), "NULL");
        return new CypherTypeException(gql, "Expected a node, but got null ");
    }

    public static CypherTypeException expectedRelButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected relationship but got %s", gotType));
    }

    public static CypherTypeException typeMismatchExpectedARel(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a relationship but was %s", got));
    }

    public static CypherTypeException expectedRel(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Relationship, got: %s", got));
    }

    public static CypherTypeException expectedOtherType(
            String got, String expectedType, String gotType, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of(expectedType), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a %s, but it was a %s", got, expectedType, gotType));
    }

    public static CypherTypeException howTreatPredicate(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat a predicate: %s", got), null);
    }

    public static CypherTypeException howTreatAsPredicate(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat that as a predicate: %s", got));
    }

    public static CypherTypeException expectedPrimitivePropertyValue(
            String got, String gotPretty, String gotCypherType, Boolean withEncountered) {
        var gql = GqlHelper.getGql22G03_22N01(
                gotPretty,
                List.of(
                        "BOOLEAN",
                        "STRING",
                        "INTEGER",
                        "FLOAT",
                        "DATE",
                        "LOCAL TIME",
                        "ZONED TIME",
                        "LOCAL DATETIME",
                        "ZONED DATETIME",
                        "DURATION",
                        "POINT",
                        "NODE",
                        "RELATIONSHIP"),
                gotCypherType);
        String msg = "Property values can only be of primitive types or arrays thereof";
        if (withEncountered) msg += String.format(". Encountered: %s.", got);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedRelValue(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected %s to be a RelationshipValue", got));
    }

    public static CypherTypeException expectedRelValueGotType(String gotType, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected RelationshipValue but got %s", gotType));
    }

    public static CypherTypeException notCollectionOrMap(
            String got, String gotPretty, String gotCypherType, Object index) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST", "MAP"), gotCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "`%s` is not a collection or a map. Element access is only possible by performing a collection "
                                + "lookup using an integer index, or by performing a map lookup using a string key (found: %s[%s])",
                        got, got, index));
    }

    public static CypherTypeException notMap(String got, String gotPretty, String gotCypherType, Object index) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "`%s` is not a map. Element access is only possible by performing a collection "
                                + "lookup by performing a map lookup using a string key (found: %s[%s])",
                        got, got, index));
    }

    public static CypherTypeException nonIntegerListIndex(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot access a list using an non-integer number index, got %s", got), null);
    }

    public static CypherTypeException addTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(gql, String.format("Cannot add `%s` and `%s`", leftTypeName, rightTypeName));
    }

    public static CypherTypeException subtractTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot subtract `%s` from `%s`", rightTypeName, leftTypeName));
    }

    public static CypherTypeException divideTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(gql, String.format("Cannot divide `%s` by `%s`", leftTypeName, rightTypeName));
    }

    public static CypherTypeException multiplyTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot multiply `%s` and `%s`", leftTypeName, rightTypeName));
    }

    public static CypherTypeException modulusTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot calculate modulus of `%s` and `%s`", leftTypeName, rightTypeName));
    }

    public static CypherTypeException powerTypeMismatch(
            String leftPretty,
            String leftTypeName,
            String rightTypeName,
            String leftCypherType,
            String rightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(leftPretty, List.of(rightCypherType), leftCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot raise `%s` to the power of `%s`", leftTypeName, rightTypeName));
    }

    public static CypherTypeException propertyParamIsNotMap(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Parameter provided for setting properties is not a Map, instead got %s", got));
    }

    @Override
    public Status status() {
        return Status.Statement.TypeError;
    }
}
