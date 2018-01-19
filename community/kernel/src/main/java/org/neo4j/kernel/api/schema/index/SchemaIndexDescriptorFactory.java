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
package org.neo4j.kernel.api.schema.index;


import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.UNIQUE;

public class SchemaIndexDescriptorFactory
{
    private SchemaIndexDescriptorFactory()
    {
    }

    public static SchemaIndexDescriptor forLabel( int labelId, int... propertyIds )
    {
        return forSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static SchemaIndexDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static SchemaIndexDescriptor forSchema( LabelSchemaDescriptor schema )
    {
        return new SchemaIndexDescriptor( schema, GENERAL );
    }

    public static SchemaIndexDescriptor uniqueForSchema( LabelSchemaDescriptor schema )
    {
        return new SchemaIndexDescriptor( schema, UNIQUE );
    }
}
