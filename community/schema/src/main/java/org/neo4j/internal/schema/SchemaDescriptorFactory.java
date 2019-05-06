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
package org.neo4j.internal.schema;

import org.neo4j.token.api.TokenConstants;

public class SchemaDescriptorFactory
{
    private SchemaDescriptorFactory()
    {
    }

    public static LabelSchemaDescriptor forLabel( int labelId, int... propertyIds )
    {
        return forLabelOfType( IndexType.ANY_GENERAL, labelId, propertyIds );
    }

    public static LabelSchemaDescriptor forLabelNoIndex( int labelId, int... propertyIds )
    {
        return forLabelOfType( IndexType.NOT_AN_INDEX, labelId, propertyIds );
    }

    public static LabelSchemaDescriptor forLabelOfType( IndexType indexType, int labelId, int... propertyIds )
    {
        validateLabelIds( labelId );
        validatePropertyIds( propertyIds );
        return new DefaultLabelSchemaDescriptor( indexType, labelId, propertyIds );
    }

    public static RelationTypeSchemaDescriptor forRelType( int relTypeId, int... propertyIds )
    {
        return forRelTypeOfType( IndexType.ANY_GENERAL, relTypeId, propertyIds );
    }

    public static RelationTypeSchemaDescriptor forRelTypeNoIndex( int relTypeId, int... propertyIds )
    {
        return forRelTypeOfType( IndexType.NOT_AN_INDEX, relTypeId, propertyIds );
    }

    public static RelationTypeSchemaDescriptor forRelTypeOfType( IndexType indexType, int relTypeId, int... propertyIds )
    {
        validateRelationshipTypeIds( relTypeId );
        validatePropertyIds( propertyIds );
        return new DefaultRelationTypeSchemaDescriptor( indexType, relTypeId, propertyIds );
    }

    private static void validatePropertyIds( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            if ( TokenConstants.ANY_PROPERTY_KEY == propertyId )
            {
                throw new IllegalArgumentException(
                        "Index schema descriptor can't be created for non existent property." );
            }
        }
    }

    private static void validateRelationshipTypeIds( int... relTypes )
    {
        for ( int relType : relTypes )
        {
            if ( TokenConstants.ANY_RELATIONSHIP_TYPE == relType )
            {
                throw new IllegalArgumentException( "Index schema descriptor can't be created for non existent relationship type." );
            }
        }
    }

    private static void validateLabelIds( int... labelIds )
    {
        for ( int labelId : labelIds )
        {
            if ( TokenConstants.ANY_LABEL == labelId )
            {
                throw new IllegalArgumentException( "Index schema descriptor can't be created for non existent label." );
            }
        }
    }
}
