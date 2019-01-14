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
package org.neo4j.kernel.api.schema;

import org.neo4j.kernel.api.StatementConstants;

public class SchemaDescriptorFactory
{
    private SchemaDescriptorFactory()
    {
    }

    public static LabelSchemaDescriptor forLabel( int labelId, int... propertyIds )
    {
        validateLabelId( labelId );
        validatePropertyIds( propertyIds );
        return new LabelSchemaDescriptor( labelId, propertyIds );
    }

    public static RelationTypeSchemaDescriptor forRelType( int relTypeId, int... propertyIds )
    {
        validateRelationshipTypeLabelId( relTypeId );
        validatePropertyIds( propertyIds );
        return new RelationTypeSchemaDescriptor( relTypeId, propertyIds );
    }

    private static void validatePropertyIds( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            if ( StatementConstants.NO_SUCH_PROPERTY_KEY == propertyId )
            {
                throw new IllegalArgumentException(
                        "Index schema descriptor can't be created for non existent property." );
            }
        }
    }

    private static void validateRelationshipTypeLabelId( int relType )
    {
        if ( StatementConstants.NO_SUCH_RELATIONSHIP_TYPE == relType )
        {
            throw new IllegalArgumentException(
                    "Index schema descriptor can't be created for non existent relationship type." );
        }
    }

    private static void validateLabelId( int labelId )
    {
        if ( StatementConstants.NO_SUCH_LABEL == labelId )
        {
            throw new IllegalArgumentException( "Index schema descriptor can't be created for non existent label." );
        }
    }
}
