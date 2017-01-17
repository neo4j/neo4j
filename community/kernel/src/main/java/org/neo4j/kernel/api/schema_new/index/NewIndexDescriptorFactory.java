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
package org.neo4j.kernel.api.schema_new.index;

import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;

import static org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor.Type.UNIQUE;

public class NewIndexDescriptorFactory
{
    public static NewIndexDescriptor forLabel( int labelId, int... propertyIds )
    {
        return new NewIndexDescriptorImpl( SchemaDescriptorFactory.forLabel( labelId, propertyIds ), GENERAL );
    }

    public static NewIndexDescriptor forRelType( int relTypeId, int... propertyIds )
    {
        return new NewIndexDescriptorImpl( SchemaDescriptorFactory.forRelType( relTypeId, propertyIds ), GENERAL );
    }

    public static NewIndexDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return new NewIndexDescriptorImpl( SchemaDescriptorFactory.forLabel( labelId, propertyIds ), UNIQUE );
    }

    public static NewIndexDescriptor uniqueForRelType( int relTypeId, int... propertyIds )
    {
        return new NewIndexDescriptorImpl( SchemaDescriptorFactory.forRelType( relTypeId, propertyIds ), UNIQUE );
    }
}
