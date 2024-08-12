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

import static java.lang.String.format;

import java.util.Locale;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Represent something gone wrong related to SchemaRules
 */
public class SchemaRuleException extends SchemaKernelException {
    private final SchemaDescriptorSupplier schemaThing;
    private final String messageTemplate;

    /**
     * @param messageTemplate Template for {@code String.format}. Must match two strings representing the schema kind and the descriptor.
     * @param schemaThing schema element relevant to this exception.
     * @param tokenNameLookup how to look up tokens for describing the given {@code schemaThing}.
     */
    SchemaRuleException(
            Status status,
            String messageTemplate,
            SchemaDescriptorSupplier schemaThing,
            TokenNameLookup tokenNameLookup) {
        super(
                status,
                format(
                        messageTemplate,
                        describe(schemaThing),
                        schemaThing.schema().userDescription(tokenNameLookup)));
        this.schemaThing = schemaThing;
        this.messageTemplate = messageTemplate;
    }

    SchemaRuleException(
            ErrorGqlStatusObject gqlStatusObject,
            Status status,
            String messageTemplate,
            SchemaDescriptorSupplier schemaThing,
            TokenNameLookup tokenNameLookup) {
        super(
                gqlStatusObject,
                status,
                format(
                        messageTemplate,
                        describe(schemaThing),
                        schemaThing.schema().userDescription(tokenNameLookup)));

        this.schemaThing = schemaThing;
        this.messageTemplate = messageTemplate;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format(
                messageTemplate, describe(schemaThing), schemaThing.schema().userDescription(tokenNameLookup));
    }

    public static String describe(SchemaDescriptorSupplier schemaThing) {
        SchemaDescriptor schema = schemaThing.schema();
        String tagType =
                switch (schema.entityType()) {
                    case NODE -> "label";
                    case RELATIONSHIP -> "relationship type";
                };

        if (schemaThing instanceof ConstraintDescriptor constraint) {
            return switch (constraint.type()) {
                case UNIQUE -> tagType + " uniqueness constraint";
                case EXISTS -> tagType + " property existence constraint";
                case UNIQUE_EXISTS -> schema.entityType().name().toLowerCase(Locale.ROOT) + " key constraint";
                default -> throw new AssertionError("Unknown constraint type: " + constraint.type());
            };
        } else if (schemaThing instanceof IndexDescriptor index) {
            return index.getIndexType().name().toLowerCase(Locale.ROOT) + " " + tagType + " index";
        }
        return tagType + " schema";
    }
}
