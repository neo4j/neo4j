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
package org.neo4j.internal.kernel.api.schema;

import java.util.Arrays;
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
    SchemaDescriptor NO_SCHEMA = new SchemaDescriptor()
    {
        @Override
        public boolean isAffected( long[] entityIds )
        {
            return false;
        }

        @Override
        public <R> R computeWith( SchemaComputer<R> computer )
        {
            return null;
        }

        @Override
        public void processWith( SchemaProcessor processor )
        {

        }

        @Override
        public String userDescription( TokenNameLookup tokenNameLookup )
        {
            return "NO_SCHEMA";
        }

        @Override
        public int[] getPropertyIds()
        {
            return new int[0];
        }

        @Override
        public int[] getEntityTokenIds()
        {
            return new int[0];
        }

        @Override
        public int keyId()
        {
            return 0;
        }

        @Override
        public ResourceType keyType()
        {
            return null;
        }

        @Override
        public EntityType entityType()
        {
            return null;
        }

        @Override
        public PropertySchemaType propertySchemaType()
        {
            return null;
        }

        @Override
        public SchemaDescriptor schema()
        {
            return null;
        }
    };

    static long[] schemaTokenLockingIds( SchemaDescriptor schema )
    {
        // TODO make getEntityTokenIds produce a long array directly, and avoid this extra copying.
        return schemaTokenLockingIds( schema.getEntityTokenIds() );
    }

    static long[] schemaTokenLockingIds( int[] tokenIds )
    {
        long[] lockingIds = new long[tokenIds.length];
        for ( int i = 0; i < lockingIds.length; i++ )
        {
            lockingIds[i] = tokenIds[i];
        }
        return lockingIds;
    }

    /**
     * Returns true if any of the given entity token ids are part of this schema unit.
     * @param entityTokenIds entity token ids to check against.
     * @return true if the supplied ids are relevant to this schema unit.
     */
    boolean isAffected( long[] entityTokenIds );

    /**
     * This enum signifies how this schema should behave in regards to updates.
     * {@link PropertySchemaType#COMPLETE_ALL_TOKENS} signifies that this schema unit only should be affected by updates that match the entire schema,
     * i.e. when all properties are present. If you are unsure then this is probably what you want.
     * {@link PropertySchemaType#PARTIAL_ANY_TOKEN} signifies that this schema unit should be affected by any update that is partial match of the schema,
     *  i.e. at least one of the properties of this schema unit is present.
     */
    enum PropertySchemaType
    {
        COMPLETE_ALL_TOKENS,
        PARTIAL_ANY_TOKEN
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
     * Assume that this schema descriptor describes a schema that includes a single property id, and return that id.
     *
     * @return The presumed single property id of this schema.
     * @throws IllegalStateException if this schema does not have exactly one property.
     */
    default int getPropertyId()
    {
        int[] propertyIds = getPropertyIds();
        if ( propertyIds.length != 1 )
        {
            throw new IllegalStateException(
                    "Single property schema requires one property but had " + propertyIds.length );
        }
        return propertyIds[0];
    }

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
    int keyId();

    /**
     * Type of underlying schema descriptor key.
     * Key is part of schema unit that determines which resources with specified properties are applicable.
     * @return type of underlying key
     */
    ResourceType keyType();

    /**
     * Type of entities this schema represents.
     * @return entity type
     */
    EntityType entityType();

    /**
     * Returns the type of this schema. See {@link PropertySchemaType}.
     * @return PropertySchemaType of this schema unit.
     */
    PropertySchemaType propertySchemaType();

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
}
