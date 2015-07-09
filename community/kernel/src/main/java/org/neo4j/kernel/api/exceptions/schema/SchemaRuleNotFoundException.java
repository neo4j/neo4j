/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class SchemaRuleNotFoundException extends SchemaKernelException
{
    private static final String NODE_RULE_NOT_FOUND_MESSAGE_TEMPLATE =
            "Index rule(s) for label '%s' and property '%s': %s.";

    private static final String RELATIONSHIP_RULE_NOT_FOUND_MESSAGE_TEMPLATE =
            "Index rule(s) for relationship type '%s' and property '%s': %s.";

    private static final int UNINITIALIZED = -1;

    private final int labelId;
    private final int relationshipTypeId;
    private final int propertyKeyId;
    private final String reason;

    private SchemaRuleNotFoundException( String messageTemplate, int labelId, int relationshipTypeId,
            int propertyKeyId, String reason )
    {
        super( Status.Schema.NoSuchSchemaRule, format( messageTemplate,
                (labelId != UNINITIALIZED) ? labelId : relationshipTypeId, propertyKeyId, reason ) );

        this.labelId = labelId;
        this.relationshipTypeId = relationshipTypeId;
        this.propertyKeyId = propertyKeyId;
        this.reason = reason;
    }

    public static SchemaRuleNotFoundException forNode( int label, int propertyKey, String message )
    {
        return new SchemaRuleNotFoundException( NODE_RULE_NOT_FOUND_MESSAGE_TEMPLATE, label, UNINITIALIZED, propertyKey,
                message );
    }

    public static SchemaRuleNotFoundException forRelationship( int type, int propertyKey, String message )
    {
        return new SchemaRuleNotFoundException( RELATIONSHIP_RULE_NOT_FOUND_MESSAGE_TEMPLATE, UNINITIALIZED, type,
                propertyKey, message );
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        String messageTemplate;
        String labelOrType;
        if ( labelId != UNINITIALIZED )
        {
            messageTemplate = NODE_RULE_NOT_FOUND_MESSAGE_TEMPLATE;
            labelOrType = tokenNameLookup.labelGetName( labelId );
        }
        else
        {
            messageTemplate = RELATIONSHIP_RULE_NOT_FOUND_MESSAGE_TEMPLATE;
            labelOrType = tokenNameLookup.relationshipTypeGetName( relationshipTypeId );
        }
        return format( messageTemplate, labelOrType, tokenNameLookup.propertyKeyGetName( propertyKeyId ), reason );
    }
}
