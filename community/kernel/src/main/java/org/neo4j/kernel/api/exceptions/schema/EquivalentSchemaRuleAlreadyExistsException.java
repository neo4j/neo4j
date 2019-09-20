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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.api.exceptions.Status;

public class EquivalentSchemaRuleAlreadyExistsException extends SchemaKernelException
{
    private static final String EQUIVALENT_SCHEMA_RULE = "An equivalent schema rule already exists, '%s'.";
    private static final String EQUIVALENT_INDEX = "An equivalent index already exists, '%s'.";
    private static final String EQUIVALENT_CONSTRAINT = "An equivalent constraint already exists, '%s'.";

    public EquivalentSchemaRuleAlreadyExistsException( SchemaRule schemaRule, OperationContext context, TokenNameLookup tokenNameLookup )
    {
        super( Status.Schema.EquivalentSchemaRuleAlreadyExists, constructUserMessage( context, tokenNameLookup, schemaRule ) );
    }

    private static String constructUserMessage( OperationContext context, TokenNameLookup tokenNameLookup, SchemaRule schemaRule )
    {
        switch ( context )
        {
        case INDEX_CREATION:
            return String.format( EQUIVALENT_INDEX, schemaRule.userDescription( tokenNameLookup ) );
        case CONSTRAINT_CREATION:
            return String.format( EQUIVALENT_CONSTRAINT, schemaRule.userDescription( tokenNameLookup ) );
        default:
            return String.format( EQUIVALENT_SCHEMA_RULE, schemaRule.userDescription( tokenNameLookup ) );
        }
    }
}
