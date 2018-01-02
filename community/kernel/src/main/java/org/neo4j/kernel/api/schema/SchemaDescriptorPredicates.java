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
package org.neo4j.kernel.api.schema;

import java.util.Optional;
import java.util.function.Predicate;

public class SchemaDescriptorPredicates
{
    private SchemaDescriptorPredicates()
    {
    }

    public static <T extends SchemaDescriptor.Supplier> Predicate<T> hasLabel( int labelId )
    {
        return supplier ->
        {
            Optional<Integer> labelOpt = supplier.schema().computeWith( getLabel );
            return labelOpt.isPresent() && labelOpt.get() == labelId;
        };
    }

    public static <T extends SchemaDescriptor.Supplier> Predicate<T> hasRelType( int relTypeId )
    {
        return supplier ->
        {
            Optional<Integer> relTypeOpt = supplier.schema().computeWith( getRelType );
            return relTypeOpt.isPresent() && relTypeOpt.get() == relTypeId;
        };
    }

    public static <T extends SchemaDescriptor.Supplier> Predicate<T> hasProperty( int propertyId )
    {
        return supplier -> hasProperty( supplier, propertyId );
    }

    public static boolean hasLabel( SchemaDescriptor.Supplier supplier, int labelId )
    {
        Optional<Integer> labelOpt = supplier.schema().computeWith( getLabel );
        return labelOpt.isPresent() && labelOpt.get() == labelId;
    }

    public static boolean hasRelType( SchemaDescriptor.Supplier supplier, int relTypeId )
    {
        Optional<Integer> relTypeOpt = supplier.schema().computeWith( getRelType );
        return relTypeOpt.isPresent() && relTypeOpt.get() == relTypeId;
    }

    public static boolean hasProperty( SchemaDescriptor.Supplier supplier, int propertyId )
    {
        int[] schemaProperties = supplier.schema().getPropertyIds();
        for ( int schemaProp : schemaProperties )
        {
            if ( schemaProp == propertyId )
            {
                return true;
            }
        }
        return false;
    }

    private static SchemaComputer<Optional<Integer>> getLabel = new SchemaComputer<Optional<Integer>>()
    {
        @Override
        public Optional<Integer> computeSpecific( LabelSchemaDescriptor schema )
        {
            return Optional.of( schema.getLabelId() );
        }

        @Override
        public Optional<Integer> computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            return Optional.empty();
        }
    };

    private static SchemaComputer<Optional<Integer>> getRelType = new SchemaComputer<Optional<Integer>>()
    {
        @Override
        public Optional<Integer> computeSpecific( LabelSchemaDescriptor schema )
        {
            return Optional.empty();
        }

        @Override
        public Optional<Integer> computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            return Optional.of( schema.getRelTypeId() );
        }
    };
}
