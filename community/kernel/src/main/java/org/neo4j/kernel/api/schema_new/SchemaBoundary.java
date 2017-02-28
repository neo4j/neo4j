/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.schema_new;

import org.neo4j.kernel.api.schema.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;

/**
 * This class represents the boundary of where new schema descriptors are converted to old descriptors. This class
 * should disappear once the old schema descriptors are no longer used.
 */
public class SchemaBoundary
{
    private SchemaBoundary()
    {
    }

    public static LabelSchemaDescriptor map( NodePropertyDescriptor descriptor )
    {
        return descriptor.isComposite() ?
               SchemaDescriptorFactory.forLabel( descriptor.getLabelId(), descriptor.getPropertyKeyIds() ) :
               SchemaDescriptorFactory.forLabel( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }

    public static RelationTypeSchemaDescriptor map( RelationshipPropertyDescriptor descriptor )
    {
        return SchemaDescriptorFactory.forRelType( descriptor.getRelationshipTypeId(), descriptor.getPropertyKeyId() );
    }

    public static NodePropertyDescriptor map( LabelSchemaDescriptor schema )
    {
        return IndexDescriptorFactory.getNodePropertyDescriptor( schema.getLabelId(), schema.getPropertyIds() );
    }
}
