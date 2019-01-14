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
package org.neo4j.kernel.api.schema.constaints;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static java.lang.String.format;

public class ConstraintDescriptorFactory
{
    private ConstraintDescriptorFactory()
    {
    }

    public static NodeExistenceConstraintDescriptor existsForLabel( int labelId, int... propertyIds )
    {
        return new NodeExistenceConstraintDescriptor( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static RelExistenceConstraintDescriptor existsForRelType( int relTypeId, int... propertyIds )
    {
        return new RelExistenceConstraintDescriptor( SchemaDescriptorFactory.forRelType( relTypeId, propertyIds ) );
    }

    public static UniquenessConstraintDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return new UniquenessConstraintDescriptor( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForLabel( int labelId, int... propertyIds )
    {
        return new NodeKeyConstraintDescriptor( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static ConstraintDescriptor existsForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToExistenceConstraint );
    }

    public static NodeExistenceConstraintDescriptor existsForSchema( LabelSchemaDescriptor schema )
    {
        return new NodeExistenceConstraintDescriptor( schema );
    }

    public static RelExistenceConstraintDescriptor existsForSchema( RelationTypeSchemaDescriptor schema )
    {
        return new RelExistenceConstraintDescriptor( schema );
    }

    public static UniquenessConstraintDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToUniquenessConstraint );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToNodeKeyConstraint );
    }

    private static SchemaComputer<ConstraintDescriptor> convertToExistenceConstraint =
            new SchemaComputer<ConstraintDescriptor>()
            {
                @Override
                public ConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
                {
                    return new NodeExistenceConstraintDescriptor( schema );
                }

                @Override
                public ConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
                {
                    return new RelExistenceConstraintDescriptor( schema );
                }
            };

    private static SchemaComputer<UniquenessConstraintDescriptor> convertToUniquenessConstraint =
            new SchemaComputer<UniquenessConstraintDescriptor>()
            {
                @Override
                public UniquenessConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
                {
                    return new UniquenessConstraintDescriptor( schema );
                }

                @Override
                public UniquenessConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
                {
                    throw new UnsupportedOperationException(
                            format( "Cannot create uniqueness constraint for schema '%s' of type %s",
                                    schema.userDescription( SchemaUtil.idTokenNameLookup ),
                                    schema.getClass().getSimpleName()
                            ) );
                }
            };

    private static SchemaComputer<NodeKeyConstraintDescriptor> convertToNodeKeyConstraint =
            new SchemaComputer<NodeKeyConstraintDescriptor>()
            {
                @Override
                public NodeKeyConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
                {
                    return new NodeKeyConstraintDescriptor( schema );
                }

                @Override
                public NodeKeyConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
                {
                    throw new UnsupportedOperationException(
                            format( "Cannot create node key constraint for schema '%s' of type %s",
                                    schema.userDescription( SchemaUtil.idTokenNameLookup ),
                                    schema.getClass().getSimpleName()
                            ) );
                }
            };
}
