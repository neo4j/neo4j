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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SchemaDescriptorPredicates
{
    public static boolean hasLabel( SchemaDescriptorSupplier supplier, int labelId )
    {
        Optional<Integer> labelOpt = getLabel.compute( supplier.getSchemaDescriptor() );
        return labelOpt.isPresent() && labelOpt.get() == labelId;
    }

    public static boolean hasRelType( SchemaDescriptorSupplier supplier, int relTypeId )
    {
        Optional<Integer> relTypeOpt = getRelType.compute( supplier.getSchemaDescriptor() );
        return relTypeOpt.isPresent() && relTypeOpt.get() == relTypeId;
    }

    public static boolean hasProperty( SchemaDescriptorSupplier supplier, int propertyId )
    {
        List<Integer> properties = getProperties.compute( supplier.getSchemaDescriptor() );
        return properties.contains( propertyId );
    }

    private static SchemaComputer<Optional<Integer>> getLabel = new SchemaComputer<Optional<Integer>>()
    {
        @Override
        public Optional<Integer> compute( LabelSchemaDescriptor schema )
        {
            return Optional.of( schema.getLabelId() );
        }

        @Override
        public Optional<Integer> compute( RelationTypeSchemaDescriptor schema )
        {
            return Optional.empty();
        }
    };

    private static SchemaComputer<Optional<Integer>> getRelType = new SchemaComputer<Optional<Integer>>()
    {
        @Override
        public Optional<Integer> compute( LabelSchemaDescriptor schema )
        {
            return Optional.empty();
        }

        @Override
        public Optional<Integer> compute( RelationTypeSchemaDescriptor schema )
        {
            return Optional.of( schema.getRelTypeId() );
        }
    };

    private static SchemaComputer<List<Integer>> getProperties = new SchemaComputer<List<Integer>>()
    {
        @Override
        public List<Integer> compute( LabelSchemaDescriptor schema )
        {
            return asList( schema.getPropertyIds() );
        }

        @Override
        public List<Integer> compute( RelationTypeSchemaDescriptor schema )
        {
            return asList( schema.getPropertyIds() );
        }
    };

    private static List<Integer> asList( int[] ints )
    {
        List<Integer> list = new ArrayList<>();
        for ( int i : ints )
        {
            list.add( i );
        }
        return list;
    }
}
