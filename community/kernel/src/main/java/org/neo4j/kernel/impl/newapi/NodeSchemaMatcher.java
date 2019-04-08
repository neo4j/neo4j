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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;

import static org.neo4j.collection.PrimitiveArrays.isSortedSet;

/**
 * This class holds functionality to match LabelSchemaDescriptors to nodes
 */
public class NodeSchemaMatcher
{
    private NodeSchemaMatcher()
    {
        throw new AssertionError( "no instance" );
    }

    /**
     * Iterate over some schema suppliers, and invoke a callback for every supplier that matches the node. To match the
     * node N the supplier must supply a LabelSchemaDescriptor D, such that N has values for all the properties of D.
     * The supplied schemas are all assumed to match N on label.
     * <p>
     * To avoid unnecessary store lookups, this implementation only gets propertyKeyIds for the node if some
     * descriptor has a valid label.
     *
     * @param <SUPPLIER> the type to match. Must implement SchemaDescriptorSupplier
     * @param <EXCEPTION> The type of exception that can be thrown when taking the action
     * @param schemaSuppliers The suppliers to match
     * @param specialPropertyId This property id will always count as a match for the descriptor, regardless of
     * whether the node has this property or not
     * @param existingPropertyIds sorted array of property ids for the entity to match schema for.
     * @param callback The action to take on match
     * @throws EXCEPTION This exception is propagated from the action
     */
    static <SUPPLIER extends SchemaDescriptorSupplier, EXCEPTION extends Exception> void onMatchingSchema(
            Iterator<SUPPLIER> schemaSuppliers,
            int specialPropertyId,
            int[] existingPropertyIds,
            ThrowingConsumer<SUPPLIER,EXCEPTION> callback
    ) throws EXCEPTION
    {
        assert isSortedSet( existingPropertyIds );
        while ( schemaSuppliers.hasNext() )
        {
            SUPPLIER schemaSupplier = schemaSuppliers.next();
            SchemaDescriptor schema = schemaSupplier.schema();

            if ( nodeHasSchemaProperties( existingPropertyIds, schema.getPropertyIds(), specialPropertyId ) )
            {
                callback.accept( schemaSupplier );
            }
        }
    }

    /**
     * Iterate over some schema suppliers, and invoke a callback for every supplier that matches the node. To match the
     * node N the supplier must supply a LabelSchemaDescriptor D, such that N has the label of D, and values for all
     * the properties of D.
     * <p>
     * To avoid unnecessary store lookups, this implementation only gets propertyKeyIds for the node if some
     * descriptor has a valid label.
     *
     * @param <SUPPLIER> the type to match. Must implement SchemaDescriptorSupplier
     * @param <EXCEPTION> The type of exception that can be thrown when taking the action
     * @param schemaSuppliers The suppliers to match
     * @param specialPropertyId This property id will always count as a match for the descriptor, regardless of
     * whether the node has this property or not
     * @param existingPropertyIds sorted array of property ids for the entity to match schema for.
     * @param callback The action to take on match
     * @throws EXCEPTION This exception is propagated from the action
     */
    static <SUPPLIER extends SchemaDescriptorSupplier, EXCEPTION extends Exception> void onMatchingSchema(
            Iterator<SUPPLIER> schemaSuppliers,
            long[] labels,
            int specialPropertyId,
            int[] existingPropertyIds,
            ThrowingConsumer<SUPPLIER,EXCEPTION> callback
    ) throws EXCEPTION
    {
        assert isSortedSet( existingPropertyIds );
        assert isSortedSet( labels );
        while ( schemaSuppliers.hasNext() )
        {
            SUPPLIER schemaSupplier = schemaSuppliers.next();
            SchemaDescriptor schema = schemaSupplier.schema();

            if ( !schema.isAffected( labels ) )
            {
                continue;
            }

            if ( nodeHasSchemaProperties( existingPropertyIds, schema.getPropertyIds(), specialPropertyId ) )
            {
                callback.accept( schemaSupplier );
            }
        }
    }

    private static boolean nodeHasSchemaProperties(
            int[] existingPropertyIds, int[] indexPropertyIds, int changedPropertyId )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( indexPropertyId != changedPropertyId && !contains( existingPropertyIds, indexPropertyId ) )
            {
                return false;
            }
        }
        return true;
    }

    private static boolean contains( int[] existingPropertyIds, int indexPropertyId )
    {
        return Arrays.binarySearch( existingPropertyIds, indexPropertyId ) >= 0;
    }
}
