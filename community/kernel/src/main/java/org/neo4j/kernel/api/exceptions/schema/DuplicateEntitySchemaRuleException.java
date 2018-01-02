/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

public class DuplicateEntitySchemaRuleException extends DuplicateSchemaRuleException
{
    private static final String DUPLICATE_NODE_RULE_MESSAGE_TEMPLATE =
            "Multiple %s found for label '%s' and property '%s'.";
    private static final String DUPLICATED_RELATIONSHIP_RULE_MESSAGE_TEMPLATE =
            "Multiple %s found for relationship type '%s' and property '%s'.";

    private EntityType entityType;

    public DuplicateEntitySchemaRuleException( EntityType entityType, int entityId, int propertyKeyId )
    {
        this( entityType, entityId, propertyKeyId, false );
    }

    public DuplicateEntitySchemaRuleException( EntityType entityType, int ruleEntityId, int propertyKeyId,
            boolean unique )
    {
        super( getMessageTemplate( entityType ), ruleEntityId, propertyKeyId,
                unique ? UNIQUE_CONSTRAINT_PREFIX : CONSTRAINT_PREFIX );
        this.entityType = entityType;
    }


    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        String entityName = EntityType.NODE == entityType ? tokenNameLookup.labelGetName( ruleEntityId ) :
                            tokenNameLookup.relationshipTypeGetName( ruleEntityId );
        return format( messageTemplate, messagePrefix, entityName,
                tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }

    private static String getMessageTemplate( EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return DUPLICATE_NODE_RULE_MESSAGE_TEMPLATE;
        case RELATIONSHIP:
            return DUPLICATED_RELATIONSHIP_RULE_MESSAGE_TEMPLATE;
        default:
            throw new IllegalArgumentException( "Schema rules for specified entityType not supported." );
        }
    }
}
