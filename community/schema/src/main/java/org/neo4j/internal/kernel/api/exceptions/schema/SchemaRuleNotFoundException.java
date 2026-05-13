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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

public class SchemaRuleNotFoundException extends SchemaRuleException {
    private static final String NOT_FOUND_MESSAGE_TEMPLATE = "No %s was found for %s.";

    private SchemaRuleNotFoundException(
            ErrorGqlStatusObject gqlStatusObject,
            SchemaDescriptorSupplier schemaThing,
            TokenNameLookup tokenNameLookup) {
        super(
                gqlStatusObject,
                Status.Schema.SchemaRuleAccessFailed,
                NOT_FOUND_MESSAGE_TEMPLATE,
                schemaThing,
                tokenNameLookup);
    }

    public static SchemaRuleNotFoundException schemaRuleNotFound(
            SchemaDescriptorSupplier schemaThing, TokenNameLookup tokenNameLookup) {
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N21)
                .withParam(GqlParams.StringParam.schemaDescrType, SchemaRuleException.describe(schemaThing))
                .withParam(
                        GqlParams.StringParam.schemaDescr, schemaThing.schema().userDescription(tokenNameLookup))
                .build();
        return new SchemaRuleNotFoundException(gql, schemaThing, tokenNameLookup);
    }

    public static SchemaRuleNotFoundException schemaRuleNotFound(
            SchemaDescriptor schema, TokenNameLookup tokenNameLookup) {
        return schemaRuleNotFound(() -> schema, tokenNameLookup);
    }
}
