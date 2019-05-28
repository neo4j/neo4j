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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexKind;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

/**
 * Represent something gone wrong related to SchemaRules
 */
class SchemaRuleException extends SchemaKernelException
{
    private final SchemaDescriptorSupplier schemaThing;
    private final String messageTemplate;

    /**
     * @param messageTemplate Template for String.format. Must match two strings representing the schema kind and the
     *                        descriptor
     */
    SchemaRuleException( Status status, String messageTemplate, SchemaDescriptorSupplier schemaThing )
    {
        super( status, format( messageTemplate, describe( schemaThing ),
                schemaThing.schema().userDescription( idTokenNameLookup ) ) );
        this.schemaThing = schemaThing;
        this.messageTemplate = messageTemplate;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( messageTemplate, describe( schemaThing ), schemaThing.schema().userDescription( tokenNameLookup ) );
    }

    public static String describe( SchemaDescriptorSupplier schemaThing )
    {
        SchemaDescriptor schema = schemaThing.schema();
        String tagType;
        switch ( schema.entityType() )
        {
        case NODE:
            tagType = "label";
            break;
        case RELATIONSHIP:
            tagType = "relationship type";
            break;
        default:
            throw new AssertionError( "Unknown entity type: " + schema.entityType() );
        }

        if ( schemaThing instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor constraint = (ConstraintDescriptor) schemaThing;
            switch ( constraint.type() )
            {
            case UNIQUE:
                return tagType + " uniqueness constraint";
            case EXISTS:
                return tagType + " property existence constraint";
            case UNIQUE_EXISTS:
                return schema.entityType().name().toLowerCase() + " key constraint";
            default:
                throw new AssertionError( "Unknown constraint type: " + constraint.type() );
            }
        }
        else
        {
            IndexType indexType = schema.getIndexType();
            if ( indexType.getKind() == IndexKind.SPECIAL )
            {
                String indexTypeName = indexType.name().toLowerCase();
                return indexTypeName + " " + tagType + " index";
            }
            else
            {
                return tagType + " index";
            }
        }
    }
}
