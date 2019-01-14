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
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;

import static java.lang.String.format;

public class NodePropertyExistenceException extends ConstraintValidationException
{
    private final long nodeId;
    private final LabelSchemaDescriptor schema;

    public NodePropertyExistenceException( LabelSchemaDescriptor schema,
            ConstraintValidationException.Phase phase, long nodeId )
    {
        super( ConstraintDescriptorFactory.existsForSchema( schema ),
                phase, format( "Node(%d)", nodeId ) );
        this.schema = schema;
        this.nodeId = nodeId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        String propertyNoun = (schema.getPropertyIds().length > 1) ? "properties" : "property";
        return format( "Node(%d) with label `%s` must have the %s `%s`",
                nodeId,
                tokenNameLookup.labelGetName( schema.getLabelId() ), propertyNoun,
                SchemaUtil.niceProperties( tokenNameLookup, schema.getPropertyIds() ) );
    }
}
