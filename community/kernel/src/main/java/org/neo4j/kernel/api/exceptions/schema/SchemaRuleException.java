/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;

/**
 * Represent something gone wrong related to SchemaRules
 */
class SchemaRuleException extends SchemaKernelException
{
    protected final SchemaDescriptor descriptor;
    protected final String messageTemplate;
    protected final SchemaRule.Kind kind;

    /**
     * @param messageTemplate Template for String.format. Must match two strings representing the schema kind and the
     *                        descriptor
     */
    protected SchemaRuleException( Status status, String messageTemplate, SchemaRule.Kind kind,
            SchemaDescriptor descriptor )
    {
        super( status, format( messageTemplate, kind.userString().toLowerCase(),
                descriptor.userDescription( SchemaUtil.idTokenNameLookup ) ) );
        this.descriptor = descriptor;
        this.messageTemplate = messageTemplate;
        this.kind = kind;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( messageTemplate, kind.userString().toLowerCase(), descriptor.userDescription( tokenNameLookup ) );
    }
}
