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

public class IndexSearchException extends Neo4jException {
    private final Status statusCode;

    private IndexSearchException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        super(gqlStatusObject, message);
        this.statusCode = status;
    }

    public static IndexSearchException indexNotFound(String indexName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N69)
                .withParam(GqlParams.StringParam.idxDescrOrName, indexName)
                .build();
        return new IndexSearchException(gql, Status.Schema.IndexNotFound, gql.getMessage());
    }

    public static IndexSearchException indexInPopulatingState(String indexName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N63)
                .withParam(GqlParams.StringParam.idx, indexName)
                .build();
        return new IndexSearchException(gql, Status.Schema.IndexNotFound, gql.getMessage());
    }

    public static IndexSearchException wrongBindingVariableType(
            String variableName, String expectedType, String actualTypeOfVariable) {
        var gqlCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                .withParam(GqlParams.StringParam.value, new GqlParams.IDENT().process(variableName))
                .withParam(GqlParams.ListParam.valueTypeList, List.of(expectedType))
                .withParam(GqlParams.StringParam.valueType, actualTypeOfVariable)
                .build();
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(gqlCause)
                .build();
        return new IndexSearchException(gql, Status.Statement.TypeError, gqlCause.getMessage());
    }

    public static IndexSearchException vectorIndexPropertyNotFound(String propertyName, String indexName) {
        var gql = GqlHelper.getGql22ND3(propertyName, indexName);
        return new IndexSearchException(gql, Status.Statement.PropertyNotFound, gql.getMessage());
    }

    @Override
    public Status status() {
        return statusCode;
    }
}
