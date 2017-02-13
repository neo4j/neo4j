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
package org.neo4j.kernel.api.schema_new.constaints;

import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;

import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.EXISTS;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.UNIQUE;

public class ConstraintDescriptorFactory
{
    public static ConstraintDescriptor existsForLabel( int labelId, int... propertyIds )
    {
        return existsForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static ConstraintDescriptor existsForRelType( int relTypeId, int... propertyIds )
    {
        return existsForSchema( SchemaDescriptorFactory.forRelType( relTypeId, propertyIds ) );
    }

    public static ConstraintDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static ConstraintDescriptor uniqueForRelType( int relTypeId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptorFactory.forRelType( relTypeId, propertyIds ) );
    }

    public static ConstraintDescriptor existsForSchema( SchemaDescriptor schema )
    {
        return new ConstraintDescriptor( schema, EXISTS );
    }

    public static ConstraintDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return new ConstraintDescriptor( schema, UNIQUE );
    }
}
