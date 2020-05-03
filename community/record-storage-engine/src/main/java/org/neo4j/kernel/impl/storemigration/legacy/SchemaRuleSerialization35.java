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
package org.neo4j.kernel.impl.storemigration.legacy;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaProcessor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.string.UTF8;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.storemigration.legacy.SchemaRuleDeserializer2_0to3_1.defaultIndexName;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

/**
 * A stripped down 3.5.x version of SchemaRuleSerialization. Used for reading schema rules from the legacy schema store during schema store migration.
 */
public class SchemaRuleSerialization35
{
    // Schema rule type
    // Legacy schema store reserves 1,2,3,4 and 5
    private static final byte INDEX_RULE = 11;
    private static final byte CONSTRAINT_RULE = 12;

    // Index type
    private static final byte GENERAL_INDEX = 31;
    private static final byte UNIQUE_INDEX = 32;

    // Constraint type
    private static final byte EXISTS_CONSTRAINT = 61;
    private static final byte UNIQUE_CONSTRAINT = 62;
    private static final byte UNIQUE_EXISTS_CONSTRAINT = 63;

    // Schema type
    private static final byte SIMPLE_LABEL = 91;
    private static final byte SIMPLE_REL_TYPE = 92;
    private static final byte GENERIC_MULTI_TOKEN_TYPE = 93;

    private static final long NO_OWNING_CONSTRAINT_YET = -1;
    private static final int LEGACY_LABEL_OR_REL_TYPE_ID = -1;

    private SchemaRuleSerialization35()
    {
    }

    // PUBLIC

    /**
     * Serialize the provided SchemaRule onto the target buffer
     *
     * @param schemaRule the SchemaRule to serialize
     */
    public static byte[] serialize( SchemaRule schemaRule, MemoryTracker memoryTracker )
    {
        if ( schemaRule instanceof IndexDescriptor )
        {
            return serialize( (IndexDescriptor) schemaRule, memoryTracker );
        }
        else if ( schemaRule instanceof ConstraintDescriptor )
        {
            return serialize( (ConstraintDescriptor) schemaRule, memoryTracker );
        }
        throw new IllegalStateException( "Unknown schema rule type: " + schemaRule.getClass() );
    }

    /**
     * Parse a SchemaRule from the provided buffer.
     *
     * @param id the id to give the returned Schema Rule
     * @param source the buffer to parse from
     * @return a SchemaRule
     * @throws MalformedSchemaRuleException if bytes in the buffer do encode a valid SchemaRule
     */
    public static SchemaRule deserialize( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        int legacyLabelOrRelTypeId = source.getInt();
        byte schemaRuleType = source.get();

        switch ( schemaRuleType )
        {
        case INDEX_RULE:
            return readIndexRule( id, source );
        case CONSTRAINT_RULE:
            return readConstraintRule( id, source );
        default:
            if ( SchemaRuleDeserializer2_0to3_1.isLegacySchemaRule( schemaRuleType ) )
            {
                return SchemaRuleDeserializer2_0to3_1.deserialize( id, legacyLabelOrRelTypeId, schemaRuleType, source );
            }
            throw new MalformedSchemaRuleException( format( "Got unknown schema rule type '%d'.", schemaRuleType ) );
        }
    }

    /**
     * Serialize the provided IndexRule onto the target buffer
     *
     * @param indexDescriptor the StoreIndexDescriptor to serialize
     * @throws IllegalStateException if the StoreIndexDescriptor is of type unique, but the owning constrain has not been set
     */
    public static byte[] serialize( IndexDescriptor indexDescriptor, MemoryTracker memoryTracker )
    {
        try ( var scopedBuffer = new HeapScopedBuffer( lengthOf( indexDescriptor ), memoryTracker ) )
        {
            var target = scopedBuffer.getBuffer();
            target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
            target.put( INDEX_RULE );

            UTF8.putEncodedStringInto( indexDescriptor.getIndexProvider().getKey(), target );
            UTF8.putEncodedStringInto( indexDescriptor.getIndexProvider().getVersion(), target );

            if ( !indexDescriptor.isUnique() )
            {
                target.put( GENERAL_INDEX );
            }
            else
            {
                target.put( UNIQUE_INDEX );

                // The owning constraint can be null. See IndexRule.getOwningConstraint()
                target.putLong( indexDescriptor.getOwningConstraintId().orElse( NO_OWNING_CONSTRAINT_YET ) );
            }

            indexDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
            UTF8.putEncodedStringInto( indexDescriptor.getName(), target );
            return target.array();
        }
    }

    /**
     * Serialize the provided ConstraintDescriptor onto the target buffer
     * @param constraint the ConstraintDescriptor to serialize
     * @throws IllegalStateException if the ConstraintDescriptor is of type unique, but the owned index has not been set
     */
    public static byte[] serialize( ConstraintDescriptor constraint, MemoryTracker memoryTracker )
    {
        try ( var scopedBuffer = new HeapScopedBuffer( lengthOf( constraint ), memoryTracker ) )
        {
            ByteBuffer target = scopedBuffer.getBuffer();
            target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
            target.put( CONSTRAINT_RULE );

            switch ( constraint.type() )
            {
            case EXISTS:
                target.put( EXISTS_CONSTRAINT );
                break;

            case UNIQUE:
                target.put( UNIQUE_CONSTRAINT );
                target.putLong( constraint.asIndexBackedConstraint().ownedIndexId() );
                break;

            case UNIQUE_EXISTS:
                target.put( UNIQUE_EXISTS_CONSTRAINT );
                target.putLong( constraint.asIndexBackedConstraint().ownedIndexId() );
                break;

            default:
                throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.", constraint.type() ) );
            }

            constraint.schema().processWith( new SchemaDescriptorSerializer( target ) );
            UTF8.putEncodedStringInto( constraint.getName(), target );
            return target.array();
        }
    }

    /**
     * Compute the byte size needed to serialize the provided IndexRule using serialize.
     * @param indexDescriptor the StoreIndexDescriptor
     * @return the byte size of StoreIndexDescriptor
     */
    static int lengthOf( IndexDescriptor indexDescriptor )
    {
        int length = 4; // legacy label or relType id
        length += 1;    // schema rule type

        length += UTF8.computeRequiredByteBufferSize( indexDescriptor.getIndexProvider().getKey() );
        length += UTF8.computeRequiredByteBufferSize( indexDescriptor.getIndexProvider().getVersion() );

        length += 1; // index type
        if ( indexDescriptor.isUnique() )
        {
            length += 8; // owning constraint id
        }

        length += computeSchemaSize( indexDescriptor.schema() );
        length += UTF8.computeRequiredByteBufferSize( indexDescriptor.getName() );
        return length;
    }

    /**
     * Compute the byte size needed to serialize the provided ConstraintDescriptor using serialize.
     * @param constraint the ConstraintDescriptor
     * @return the byte size of ConstraintDescriptor
     */
    static int lengthOf( ConstraintDescriptor constraint )
    {
        int length = 4; // legacy label or relType id
        length += 1; // schema rule type

        length += 1; // constraint type
        if ( constraint.enforcesUniqueness() )
        {
            length += 8; // owned index id
        }

        length += computeSchemaSize( constraint.schema() );
        length += UTF8.computeRequiredByteBufferSize( constraint.getName() );
        return length;
    }

    // PRIVATE

    // READ INDEX

    private static IndexDescriptor readIndexRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        String providerKey = getDecodedStringFrom( source );
        String providerVersion = getDecodedStringFrom( source );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( providerKey, providerVersion );
        byte indexRuleType = source.get();
        Optional<String> name;
        switch ( indexRuleType )
        {
        case GENERAL_INDEX:
        {
            SchemaDescriptor schema = readSchema( source );
            name = readRuleName( source );
            IndexPrototype prototype = IndexPrototype.forSchema( schema, providerDescriptor );
            if ( schema.isFulltextSchemaDescriptor() )
            {
                prototype = prototype.withIndexType( IndexType.FULLTEXT );
            }
            if ( name.isPresent() )
            {
                prototype = prototype.withName( name.get() );
            }
            else
            {
                prototype = prototype.withName( defaultIndexName( id ) );
            }
            return prototype.materialise( id );
        }
        case UNIQUE_INDEX:
        {
            long readOwningConstraint = source.getLong();
            SchemaDescriptor schema = readSchema( source );
            name = readRuleName( source );

            IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema, providerDescriptor );
            if ( name.isPresent() )
            {
                prototype = prototype.withName( name.get() );
            }
            else
            {
                prototype = prototype.withName( defaultIndexName( id ) );
            }
            IndexDescriptor index = prototype.materialise( id );
            if ( readOwningConstraint != NO_OWNING_CONSTRAINT_YET )
            {
                index = index.withOwningConstraintId( readOwningConstraint );
            }
            return index;
        }
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown index rule type '%d'.", indexRuleType ) );
        }
    }

    // READ CONSTRAINT

    private static ConstraintDescriptor readConstraintRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schema;
        byte constraintRuleType = source.get();
        String name;
        switch ( constraintRuleType )
        {
        case EXISTS_CONSTRAINT:
            schema = readSchema( source );
            name = readRuleName( source ).orElse( null );
            return ConstraintDescriptorFactory.existsForSchema( schema ).withId( id ).withName( name );

        case UNIQUE_CONSTRAINT:
            long ownedUniqueIndex = source.getLong();
            schema = readSchema( source );
            UniquenessConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForSchema( schema );
            name = readRuleName( source ).orElse( null );
            return descriptor.withId( id ).withOwnedIndexId( ownedUniqueIndex ).withName( name );

        case UNIQUE_EXISTS_CONSTRAINT:
            long ownedNodeKeyIndex = source.getLong();
            schema = readSchema( source );
            NodeKeyConstraintDescriptor nodeKeyConstraintDescriptor = ConstraintDescriptorFactory.nodeKeyForSchema( schema );
            name = readRuleName( source ).orElse( null );
            return nodeKeyConstraintDescriptor.withId( id ).withOwnedIndexId( ownedNodeKeyIndex ).withName( name );

        default:
            throw new MalformedSchemaRuleException( format( "Got unknown constraint rule type '%d'.", constraintRuleType ) );
        }
    }

    private static Optional<String> readRuleName( ByteBuffer source )
    {
        if ( source.remaining() >= UTF8.MINIMUM_SERIALISED_LENGTH_BYTES )
        {
            String ruleName = UTF8.getDecodedStringFrom( source );
            return ruleName.isEmpty() ? Optional.empty() : Optional.of( ruleName );
        }
        return Optional.empty();
    }

    // READ HELP

    private static SchemaDescriptor readSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        int[] propertyIds;
        byte schemaDescriptorType = source.get();
        switch ( schemaDescriptorType )
        {
        case SIMPLE_LABEL:
            int labelId = source.getInt();
            propertyIds = readTokenIdList( source );
            return SchemaDescriptor.forLabel( labelId, propertyIds );
        case SIMPLE_REL_TYPE:
            int relTypeId = source.getInt();
            propertyIds = readTokenIdList( source );
            return SchemaDescriptor.forRelType( relTypeId, propertyIds );
        case GENERIC_MULTI_TOKEN_TYPE:
            return readMultiTokenSchema( source );
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown schema descriptor type '%d'.",
                    schemaDescriptorType ) );
        }
    }

    private static SchemaDescriptor readMultiTokenSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        byte schemaDescriptorType = source.get();
        EntityType type;
        switch ( schemaDescriptorType )
        {
        case SIMPLE_LABEL:
            type = EntityType.NODE;
            break;
        case SIMPLE_REL_TYPE:
            type = EntityType.RELATIONSHIP;
            break;
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown schema descriptor type '%d'.", schemaDescriptorType ) );
        }
        int[] entityTokenIds = readTokenIdList( source );
        int[] propertyIds = readTokenIdList( source );
        return SchemaDescriptor.fulltext( type, entityTokenIds, propertyIds );
    }

    private static int[] readTokenIdList( ByteBuffer source )
    {
        short numProperties = source.getShort();
        int[] propertyIds = new int[numProperties];
        for ( int i = 0; i < numProperties; i++ )
        {
            propertyIds[i] = source.getInt();
        }
        return propertyIds;
    }

    // WRITE

    private static class SchemaDescriptorSerializer implements SchemaProcessor
    {
        private final ByteBuffer target;

        SchemaDescriptorSerializer( ByteBuffer target )
        {
            this.target = target;
        }

        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            target.put( SIMPLE_LABEL );
            target.putInt( schema.getLabelId() );
            putIds( schema.getPropertyIds() );
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            target.put( SIMPLE_REL_TYPE );
            target.putInt( schema.getRelTypeId() );
            putIds( schema.getPropertyIds() );
        }

        @Override
        public void processSpecific( SchemaDescriptor schema )
        {
            target.put( GENERIC_MULTI_TOKEN_TYPE );
            if ( schema.entityType() == EntityType.NODE )
            {
                target.put( SIMPLE_LABEL );
            }
            else
            {
                target.put( SIMPLE_REL_TYPE );
            }

            putIds( schema.getEntityTokenIds() );
            putIds( schema.getPropertyIds() );
        }

        private void putIds( int[] ids )
        {
            target.putShort( (short) ids.length );
            for ( int entityTokenId : ids )
            {
                target.putInt( entityTokenId );
            }
        }
    }

    // LENGTH OF

    private static int computeSchemaSize( SchemaDescriptor schema )
    {
        if ( schema.isLabelSchemaDescriptor() )
        {
            return    1 // schema descriptor type
                    + 4 // label id
                    + 2 // property id count
                    + 4 * schema.getPropertyIds().length; // the actual property ids
        }
        if ( schema.isRelationshipTypeSchemaDescriptor() )
        {
            return    1 // schema descriptor type
                    + 4 // rel type id
                    + 2 // property id count
                    + 4 * schema.getPropertyIds().length; // the actual property ids
        }
        return    1 // schema descriptor type
                + 1 // entity token type
                + 2 // entity token count
                + 4 * schema.getEntityTokenIds().length // the actual property ids
                + 2 // property id count
                + 4 * schema.getPropertyIds().length; // the actual property ids
    }
}
