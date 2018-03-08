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
package org.neo4j.internal.kernel.api.schema;

import java.util.function.Predicate;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * Internal representation of one schema unit, for example a label-property pair.
 *
 * Even when only supporting a small set of different schemas, the number of common methods is very small. This
 * interface therefore supports a visitor type access pattern, results can be computed using the {#compute} method, and
 * side-effect type logic performed using the processWith method. This means that when implementing this interface
 * with a new concrete type, the compute and processWith method implementations need to be added similarly to
 * how this is done in eg. LabelSchemaDescriptor, and the SchemaProcessor and SchemaComputer interfaces need to be
 * extended with methods taking the new concrete type as argument.
 */
public interface SchemaDescriptor extends SchemaDescriptorSupplier
{
    boolean isAffected( long[] entityIds );

    enum PropertySchemaType
    {
        SCHEMA_ALL_TOKENS,
        NON_SCHEMA_ANY_TOKEN
    }
    /**
     * Computes some value by feeding this object into the given SchemaComputer.
     *
     * Note that implementers of this method just need to call `return computer.compute( this );`.
     *
     * @param computer The SchemaComputer that hold the logic for the computation
     * @param <R> The return type
     * @return The result of the computation
     */
    <R> R computeWith( SchemaComputer<R> computer );

    /**
     * Performs some side-effect type logic by processing this object using the given SchemaProcessor.
     *
     * Note that implementers of this method just need to call `return processor.process( this );`.
     *
     * @param processor The SchemaProcessor that hold the logic for the computation
     */
    void processWith( SchemaProcessor processor );

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * This method return the property ids that are relevant to this Schema Descriptor.
     *
     * Putting this method here is a convenience that will break if/when we introduce more complicated schema
     * descriptors like paths, but until that point it is very useful.
     *
     * @return the property ids
     */
    int[] getPropertyIds();

    /**
     * This method returns the entity token ids handled by this descriptor.
     * @return the entity token ids that this schema descriptor represents
     */
    int[] getEntityTokenIds();

    /**
     * Id of underlying schema descriptor key.
     * Key is part of schema unit that determines which resources with specified properties are applicable.
     * @return id of underlying key
     */
    //TODO MultiTokenSchema support for the new kernel api. This method is only used in the AllStoreHolder
    int keyId();

    /**
     * Type of underlying schema descriptor key.
     * Key is part of schema unit that determines which resources with specified properties are applicable.
     * @return type of underlying key
     */
    ResourceType keyType();

    /**
     * Create a predicate that checks whether a schema descriptor Supplier supplies the given schema descriptor.
     * @param descriptor The schema descriptor to check equality with.
     * @return A predicate that returns {@code true} if it is given a schema descriptor supplier that supplies the
     * same schema descriptor as the given schema descriptor.
     */
    static <T extends SchemaDescriptorSupplier> Predicate<T> equalTo( SchemaDescriptor descriptor )
    {
        return supplier -> descriptor.equals( supplier.schema() );
    }

    EntityType entityType();

    PropertySchemaType propertySchemaType();
}
