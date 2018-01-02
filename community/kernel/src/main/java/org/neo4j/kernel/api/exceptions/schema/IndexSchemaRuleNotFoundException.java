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

import org.neo4j.kernel.api.TokenNameLookup;

public class IndexSchemaRuleNotFoundException extends SchemaRuleNotFoundException
{
    private static final String UNIQUE_INDEX_PREFIX = "Uniqueness index";
    private static final String INDEX_PREFIX = "Index";
    private static final String INDEX_RULE_NOT_FOUND_MESSAGE_TEMPLATE =
            "%s for label '%s' and property '%s' not found.";

    public IndexSchemaRuleNotFoundException( int labelId, int propertyKeyId )
    {
        this( labelId, propertyKeyId, false );
    }

    public IndexSchemaRuleNotFoundException( int labelId, int propertyKeyId, boolean unique )
    {
        super( INDEX_RULE_NOT_FOUND_MESSAGE_TEMPLATE, labelId, propertyKeyId,
                unique ? UNIQUE_INDEX_PREFIX : INDEX_PREFIX );
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return String.format( messageTemplate, messagePrefix,
                tokenNameLookup.labelGetName( ruleEntityId ),
                tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }
}
