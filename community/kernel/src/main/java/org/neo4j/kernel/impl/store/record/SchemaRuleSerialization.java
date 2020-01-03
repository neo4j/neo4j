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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.string.UTF8;

import static java.lang.String.format;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

public class SchemaRuleSerialization
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

    private SchemaRuleSerialization()
    {
    }

    // PUBLIC

    /**
     * Serialize the provided SchemaRule onto the target buffer
     *
     * @param schemaRule the SchemaRule to serialize
     */
    public static byte[] serialize( SchemaRule schemaRule )
    {
        if ( schemaRule instanceof StoreIndexDescriptor )
        {
            return serialize( (StoreIndexDescriptor)schemaRule );
        }
        else if ( schemaRule instanceof ConstraintRule )
        {
            return serialize( (ConstraintRule)schemaRule );
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
    public static byte[] serialize( StoreIndexDescriptor indexDescriptor )
    {
        ByteBuffer target = ByteBuffer.allocate( lengthOf( indexDescriptor ) );
        target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
        target.put( INDEX_RULE );

        IndexProviderDescriptor providerDescriptor = indexDescriptor.providerDescriptor();
        UTF8.putEncodedStringInto( providerDescriptor.getKey(), target );
        UTF8.putEncodedStringInto( providerDescriptor.getVersion(), target );

        switch ( indexDescriptor.type() )
        {
        case GENERAL:
            target.put( GENERAL_INDEX );
            break;

        case UNIQUE:
            target.put( UNIQUE_INDEX );

            // The owning constraint can be null. See IndexRule.getOwningConstraint()
            Long owningConstraint = indexDescriptor.getOwningConstraint();
            target.putLong( owningConstraint == null ? NO_OWNING_CONSTRAINT_YET : owningConstraint );
            break;

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.",
                    indexDescriptor.type() ) );
        }

        indexDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
        UTF8.putEncodedStringInto( indexDescriptor.getName(), target );
        return target.array();
    }

    /**
     * Serialize the provided ConstraintRule onto the target buffer
     * @param constraintRule the ConstraintRule to serialize
     * @throws IllegalStateException if the ConstraintRule is of type unique, but the owned index has not been set
     */
    public static byte[] serialize( ConstraintRule constraintRule )
    {
        ByteBuffer target = ByteBuffer.allocate( lengthOf( constraintRule ) );
        target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
        target.put( CONSTRAINT_RULE );

        ConstraintDescriptor constraintDescriptor = constraintRule.getConstraintDescriptor();
        switch ( constraintDescriptor.type() )
        {
        case EXISTS:
            target.put( EXISTS_CONSTRAINT );
            break;

        case UNIQUE:
            target.put( UNIQUE_CONSTRAINT );
            target.putLong( constraintRule.getOwnedIndex() );
            break;

        case UNIQUE_EXISTS:
            target.put( UNIQUE_EXISTS_CONSTRAINT );
            target.putLong( constraintRule.getOwnedIndex() );
            break;

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.",
                    constraintDescriptor.type() ) );
        }

        constraintDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
        UTF8.putEncodedStringInto( constraintRule.getName(), target );
        return target.array();
    }

    /**
     * Compute the byte size needed to serialize the provided IndexRule using serialize.
     * @param indexDescriptor the StoreIndexDescriptor
     * @return the byte size of StoreIndexDescriptor
     */
    static int lengthOf( StoreIndexDescriptor indexDescriptor )
    {
        int length = 4; // legacy label or relType id
        length += 1;    // schema rule type

        IndexProviderDescriptor providerDescriptor = indexDescriptor.providerDescriptor();
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() );
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() );

        length += 1; // index type
        if ( indexDescriptor.type() == IndexDescriptor.Type.UNIQUE )
        {
            length += 8; // owning constraint id
        }

        length += indexDescriptor.schema().computeWith( schemaSizeComputer );
        length += UTF8.computeRequiredByteBufferSize( indexDescriptor.getName() );
        return length;
    }

    /**
     * Compute the byte size needed to serialize the provided ConstraintRule using serialize.
     * @param constraintRule the ConstraintRule
     * @return the byte size of ConstraintRule
     */
    static int lengthOf( ConstraintRule constraintRule )
    {
        int length = 4; // legacy label or relType id
        length += 1; // schema rule type

        length += 1; // constraint type
        ConstraintDescriptor constraintDescriptor = constraintRule.getConstraintDescriptor();
        if ( constraintDescriptor.enforcesUniqueness() )
        {
            length += 8; // owned index id
        }

        length += constraintDescriptor.schema().computeWith( schemaSizeComputer );
        length += UTF8.computeRequiredByteBufferSize( constraintRule.getName() );
        return length;
    }

    // PRIVATE

    // READ INDEX

    private static StoreIndexDescriptor readIndexRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        IndexProviderDescriptor indexProvider = readIndexProviderDescriptor( source );
        byte indexRuleType = source.get();
        Optional<String> name;
        switch ( indexRuleType )
        {
        case GENERAL_INDEX:
        {
            SchemaDescriptor schema = readSchema( source );
            name = readRuleName( source );
            return IndexDescriptorFactory.forSchema( schema, name, indexProvider ).withId( id );
        }
        case UNIQUE_INDEX:
        {
            long owningConstraint = source.getLong();
            SchemaDescriptor schema = readSchema( source );
            name = readRuleName( source );
            IndexDescriptor descriptor = IndexDescriptorFactory.uniqueForSchema( schema, name, indexProvider );
            return owningConstraint == NO_OWNING_CONSTRAINT_YET ? descriptor.withId( id ) : descriptor.withIds( id, owningConstraint );
        }
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown index rule type '%d'.", indexRuleType ) );
        }

    }

    private static IndexProviderDescriptor readIndexProviderDescriptor( ByteBuffer source )
    {
        String providerKey = getDecodedStringFrom( source );
        String providerVersion = getDecodedStringFrom( source );
        return new IndexProviderDescriptor( providerKey, providerVersion );
    }

    // READ CONSTRAINT

    private static ConstraintRule readConstraintRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schema;
        byte constraintRuleType = source.get();
        String name;
        switch ( constraintRuleType )
        {
        case EXISTS_CONSTRAINT:
            schema = readSchema( source );
            name = readRuleName( source ).orElse( null );
            return ConstraintRule.constraintRule( id, ConstraintDescriptorFactory.existsForSchema( schema ), name );

        case UNIQUE_CONSTRAINT:
            long ownedUniqueIndex = source.getLong();
            schema = readSchema( source );
            UniquenessConstraintDescriptor descriptor = ConstraintDescriptorFactory.uniqueForSchema( schema );
            name = readRuleName( source ).orElse( null );
            return ConstraintRule.constraintRule( id, descriptor, ownedUniqueIndex, name );

        case UNIQUE_EXISTS_CONSTRAINT:
            long ownedNodeKeyIndex = source.getLong();
            schema = readSchema( source );
            NodeKeyConstraintDescriptor nodeKeyConstraintDescriptor = ConstraintDescriptorFactory.nodeKeyForSchema( schema );
            name = readRuleName( source ).orElse( null );
            return ConstraintRule.constraintRule( id, nodeKeyConstraintDescriptor, ownedNodeKeyIndex, name );

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
            return SchemaDescriptorFactory.forLabel( labelId, propertyIds );
        case SIMPLE_REL_TYPE:
            int relTypeId = source.getInt();
            propertyIds = readTokenIdList( source );
            return SchemaDescriptorFactory.forRelType( relTypeId, propertyIds );
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
        return SchemaDescriptorFactory.multiToken( entityTokenIds, type, propertyIds );
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

    private static SchemaComputer<Integer> schemaSizeComputer = new SchemaComputer<Integer>()
    {
        @Override
        public Integer computeSpecific( LabelSchemaDescriptor schema )
        {
            return     1 // schema descriptor type
                     + 4 // label id
                     + 2 // property id count
                     + 4 * schema.getPropertyIds().length; // the actual property ids
        }

        @Override
        public Integer computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            return    1 // schema descriptor type
                    + 4 // rel type id
                    + 2 // property id count
                    + 4 * schema.getPropertyIds().length; // the actual property ids
        }

        @Override
        public Integer computeSpecific( SchemaDescriptor schema )
        {
            return    1 // schema descriptor type
                    + 1 // entity token type
                    + 2 // entity token count
                    + 4 * schema.getEntityTokenIds().length // the actual property ids
                    + 2 // property id count
                    + 4 * schema.getPropertyIds().length; // the actual property ids
        }
    };
}
