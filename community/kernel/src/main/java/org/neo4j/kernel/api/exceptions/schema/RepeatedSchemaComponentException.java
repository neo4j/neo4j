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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public abstract class RepeatedSchemaComponentException extends SchemaKernelException {
    private final SchemaDescriptor schema;
    private final OperationContext context;
    private final SchemaComponent component;

    RepeatedSchemaComponentException(
            Status status,
            SchemaDescriptor schema,
            OperationContext context,
            SchemaComponent component,
            TokenNameLookup tokenNameLookup) {
        super(status, format(schema, context, tokenNameLookup, component));
        this.schema = schema;
        this.context = context;
        this.component = component;
    }

    RepeatedSchemaComponentException(
            ErrorGqlStatusObject gqlStatusObject,
            Status status,
            SchemaDescriptor schema,
            OperationContext context,
            SchemaComponent component,
            TokenNameLookup tokenNameLookup) {
        super(gqlStatusObject, status, format(schema, context, tokenNameLookup, component));

        this.schema = schema;
        this.context = context;
        this.component = component;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format(schema, context, tokenNameLookup, component);
    }

    enum SchemaComponent {
        PROPERTY("property"),
        LABEL("label"),
        RELATIONSHIP_TYPE("relationship type");

        private final String name;

        SchemaComponent(String name) {
            this.name = name;
        }
    }

    private static String format(
            SchemaDescriptor schema,
            OperationContext context,
            TokenNameLookup tokenNameLookup,
            SchemaComponent component) {
        String schemaName =
                switch (context) {
                    case INDEX_CREATION -> "Index";
                    case CONSTRAINT_CREATION -> "Constraint";
                };
        return String.format(
                "%s on %s includes a %s more than once.",
                schemaName, schema.userDescription(tokenNameLookup), component.name);
    }
}
