/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.schema;

import java.util.function.Predicate;

import org.neo4j.common.EntityType;
import org.neo4j.lock.ResourceType;
import org.neo4j.token.api.TokenConstants;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.PropertySchemaType.COMPLETE_ALL_TOKENS;
import static org.neo4j.internal.schema.PropertySchemaType.PARTIAL_ANY_TOKEN;

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
    static SchemaDescriptor noSchema()
    {
        return NoSchemaDescriptor.NO_SCHEMA;
    }

    static FulltextSchemaDescriptor fulltext( EntityType entityType, int[] entityTokenIds, int[] propertyKeyIds )
    {
        return new SchemaDescriptorImplementation( entityType, PARTIAL_ANY_TOKEN, entityTokenIds, propertyKeyIds );
    }

    static LabelSchemaDescriptor forLabel( int labelId, int... propertyIds )
    {
        validateLabelIds( labelId );
        validatePropertyIds( propertyIds );
        return new SchemaDescriptorImplementation( NODE, COMPLETE_ALL_TOKENS, new int[]{labelId}, propertyIds );
    }

    static RelationTypeSchemaDescriptor forRelType( int relTypeId, int... propertyIds )
    {
        validateRelationshipTypeIds( relTypeId );
        validatePropertyIds( propertyIds );
        return new SchemaDescriptorImplementation( RELATIONSHIP, COMPLETE_ALL_TOKENS, new int[]{relTypeId}, propertyIds );
    }

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

    private static void validatePropertyIds( int[] propertyIds )
    {
        for ( int propertyId : propertyIds )
        {
            if ( TokenConstants.ANY_PROPERTY_KEY == propertyId )
            {
                throw new IllegalArgumentException(
                        "Index schema descriptor can't be created for non existent property." );
            }
        }
    }

    private static void validateRelationshipTypeIds( int... relTypes )
    {
        for ( int relType : relTypes )
        {
            if ( TokenConstants.ANY_RELATIONSHIP_TYPE == relType )
            {
                throw new IllegalArgumentException( "Index schema descriptor can't be created for non existent relationship type." );
            }
        }
    }

    private static void validateLabelIds( int... labelIds )
    {
        for ( int labelId : labelIds )
        {
            if ( TokenConstants.ANY_LABEL == labelId )
            {
                throw new IllegalArgumentException( "Index schema descriptor can't be created for non existent label." );
            }
        }
    }

    private static long[] schemaTokenLockingIds( SchemaDescriptor schema )
    {
        // TODO make getEntityTokenIds produce a long array directly, and avoid this extra copying.
        int[] tokenIds = schema.getEntityTokenIds();
        int length = tokenIds.length;
        long[] lockingIds = new long[length];
        for ( int i = 0; i < length; i++ )
        {
            lockingIds[i] = tokenIds[i];
        }
        return lockingIds;
    }

    /**
     * Test if this schema descriptor is a {@link LabelSchemaDescriptor}.
     * @return {@code true} if calling {@link #asLabelSchemaDescriptor()} will not throw an exception.
     */
    boolean isLabelSchemaDescriptor();

    /**
     * If this schema descriptor matches the structure required by {@link LabelSchemaDescriptor}, then return this descriptor as that type.
     * Otherwise, throw an {@link IllegalStateException}.
     */
    LabelSchemaDescriptor asLabelSchemaDescriptor();

    /**
     * Test if this schema descriptor is a {@link RelationTypeSchemaDescriptor}.
     * @return {@code true} if calling {@link #asRelationshipTypeSchemaDescriptor()} will not throw an exception.
     */
    boolean isRelationshipTypeSchemaDescriptor();

    /**
     * If this schema descriptor matches the structure required by {@link RelationTypeSchemaDescriptor}, then return this descriptor as that type.
     * Otherwise, throw an {@link IllegalStateException}.
     */
    RelationTypeSchemaDescriptor asRelationshipTypeSchemaDescriptor();

    /**
     * Test if this schema descriptor is a {@link FulltextSchemaDescriptor}.
     * @return {@code true} if calling {@link #asFulltextSchemaDescriptor()} will not throw an exception.
     */
    boolean isFulltextSchemaDescriptor();

    /**
     * If this schema descriptor matches the structure required by {@link FulltextSchemaDescriptor}, then return this descriptor as that type.
     * Otherwise, throw an {@link IllegalStateException}.
     */
    FulltextSchemaDescriptor asFulltextSchemaDescriptor();

    /**
     * Returns true if any of the given entity token ids are part of this schema unit.
     * @param entityTokenIds entity token ids to check against.
     * @return true if the supplied ids are relevant to this schema unit.
     */
    boolean isAffected( long[] entityTokenIds );

    /**
     * Performs some side-effect type logic by processing this object using the given SchemaProcessor.
     *
     * Note that implementers of this method just need to call `return processor.process( this );`.
     *
     * @param processor The SchemaProcessor that hold the logic for the computation
     */
    void processWith( SchemaProcessor processor );

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
     * Assuming this schema descriptor represents a schema on nodes, with a single label id, then get that label id.
     * Otherwise an exception is thrown.
     */
    default int getLabelId()
    {
        if ( entityType() != EntityType.NODE )
        {
            throw new IllegalStateException( "Cannot get label id from a schema on " + entityType() + " entities." );
        }
        int[] entityTokenIds = getEntityTokenIds();
        if ( entityTokenIds.length != 1 )
        {
            throw new IllegalStateException( "Cannot get a single label id from a multi-token schema descriptor: " + this );
        }
        return entityTokenIds[0];
    }

    /**
     * Assuming this schema descriptor represents a schema on relationships, with a single relationship type id, then get that relationship type id.
     * Otherwise an exception is thrown.
     */
    default int getRelTypeId()
    {
        if ( entityType() != EntityType.RELATIONSHIP )
        {
            throw new IllegalStateException( "Cannot get relationship type id from a schema on " + entityType() + " entities." );
        }
        int[] entityTokenIds = getEntityTokenIds();
        if ( entityTokenIds.length != 1 )
        {
            throw new IllegalStateException( "Cannot get a single relationship type id from a multi-token schema descriptor: " + this );
        }
        return entityTokenIds[0];
    }

    /**
     * Get the ids that together with the {@link #keyType()} can be used to acquire the schema locks needed to lock the schema represented by this descriptor.
     */
    default long[] lockingKeys()
    {
        return schemaTokenLockingIds( this );
    }

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
}
