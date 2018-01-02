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

import static java.lang.String.format;

public class DuplicateIndexSchemaRuleException extends DuplicateSchemaRuleException
{
    private static final String UNIQUE_INDEX_PREFIX = "uniqueness indexes";
    private static final String INDEX_PREFIX = "indexes";

    private static final String DUPLICATE_INDEX_RULE_MESSAGE_TEMPLATE =
            "Multiple %s found for label '%s' and property '%s'.";

    public DuplicateIndexSchemaRuleException( int ruleEntityId, int propertyKeyId,
            boolean unique )
    {
        super( DUPLICATE_INDEX_RULE_MESSAGE_TEMPLATE, ruleEntityId, propertyKeyId,
                unique ? UNIQUE_INDEX_PREFIX : INDEX_PREFIX );
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( messageTemplate, messagePrefix,
                tokenNameLookup.labelGetName( ruleEntityId ),
                tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }
}
