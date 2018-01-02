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

public class EntitySchemaRuleNotFoundException extends SchemaRuleNotFoundException
{
    private static final String NODE_RULE_NOT_FOUND_MESSAGE_TEMPLATE =
            "%s for label '%s' and property '%s' not found.";
    private static final String RELATIONSHIP_RULE_NOT_FOUND_MESSAGE_TEMPLATE =
            "%s for relationship type '%s' and property '%s' not found.";
    private EntityType entityType;

    public EntitySchemaRuleNotFoundException( EntityType entityType, int labelId, int propertyKeyId )
    {
        this( entityType, labelId, propertyKeyId, false );
    }

    public EntitySchemaRuleNotFoundException( EntityType entityType, int entityId, int propertyKeyId, boolean unique )
    {
        super( getMessageTemplate( entityType ), entityId, propertyKeyId,
                unique ? UNIQUE_CONSTRAINT_PREFIX : CONSTRAINT_PREFIX );
        this.entityType = entityType;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        String entityName = EntityType.NODE == entityType ? tokenNameLookup.labelGetName( ruleEntityId ) :
                            tokenNameLookup.relationshipTypeGetName( ruleEntityId );
        return String.format( messageTemplate, messagePrefix, entityName,
                tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }

    private static String getMessageTemplate( EntityType entityType )
    {
        switch ( entityType )
        {
        case NODE:
            return NODE_RULE_NOT_FOUND_MESSAGE_TEMPLATE;
        case RELATIONSHIP:
            return RELATIONSHIP_RULE_NOT_FOUND_MESSAGE_TEMPLATE;
        default:
            throw new IllegalArgumentException( "Schema rules for specified entityType not supported." );
        }
    }
}
