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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type;
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
    private static final byte NON_SCHEMA_INDEX = 33;

    // Constraint type
    private static final byte EXISTS_CONSTRAINT = 61;
    private static final byte UNIQUE_CONSTRAINT = 62;
    private static final byte UNIQUE_EXISTS_CONSTRAINT = 63;

    // Schema type
    private static final byte SIMPLE_LABEL = 91;
    private static final byte SIMPLE_REL_TYPE = 92;

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
        public Integer computeSpecific( MultiTokenSchemaDescriptor schema )
        {
            return 1 // schema descriptor type
                    + 2 // entity token count
                    + 4 * schema.getEntityTokenIds().length // the actual property ids
                    + 2 // property id count
                    + 4 * schema.getPropertyIds().length; // the actual property ids
        }
    };

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
    public static int lengthOf( StoreIndexDescriptor indexDescriptor )
    {
        int length = 4; // legacy label or relType id
        length += 1;    // schema rule type

        IndexProvider.Descriptor providerDescriptor = indexDescriptor.providerDescriptor();
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() );
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() );

        length += 1; // index type
        if ( indexRule.type() == Type.UNIQUE )
        {
            length += 8; // owning constraint id
        }

        length += indexRule.schema().computeWith( schemaSizeComputer );
        length += UTF8.computeRequiredByteBufferSize( indexRule.getName() );
        length += UTF8.computeRequiredByteBufferSize( indexRule.getMetadata() );
        return length;
    }

    /**
     * Compute the byte size needed to serialize the provided ConstraintRule using serialize.
     * @param constraintRule the ConstraintRule
     * @return the byte size of ConstraintRule
     */
    public static int lengthOf( ConstraintRule constraintRule )
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
        IndexProvider.Descriptor indexProvider = readIndexProviderDescriptor( source );
        byte indexRuleType = source.get();
        Optional<String> name;
        switch ( indexRuleType )
        {
        case GENERAL_INDEX:
        {
            LabelSchemaDescriptor schema = readLabelSchema( source );
            name = readRuleName( id, IndexRule.class, source );
            return IndexRule.forSchema( id, schema ).withProvider( indexProvider ).withName( name ).build();
        }
        case UNIQUE_INDEX:
        {
            long owningConstraint = source.getLong();
            LabelSchemaDescriptor schema = readLabelSchema( source );
            SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.uniqueForSchema( schema );
            name = readRuleName( id, IndexRule.class, source );
            Long constraint = owningConstraint == NO_OWNING_CONSTRAINT_YET ? null : owningConstraint;
            return IndexRule.forIndex( id, descriptor ).withProvider( indexProvider ).withName( name ).withOwingConstraint( constraint ).build();
        }
        case NON_SCHEMA_INDEX:
        {
            MultiTokenSchemaDescriptor nonSchema = readNonSchemaSchema( source );
            name = readRuleName( id, IndexRule.class, source );
            String metadata = readMetaData( source );
            return IndexRule.forSchema( id, nonSchema ).withProvider( indexProvider ).withName( name ).withMetadata( metadata ).withType(
                    Type.NON_SCHEMA ).build();
        }

        default:
            throw new MalformedSchemaRuleException( format( "Got unknown index rule type '%d'.", indexRuleType ) );
        }
    }

    private static LabelSchemaDescriptor readLabelSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schemaDescriptor = readSchema( source );
        if ( !(schemaDescriptor instanceof LabelSchemaDescriptor) )
        {
            throw new MalformedSchemaRuleException( "Schema IndexRules must have LabelSchemaDescriptors, got " +
                    schemaDescriptor.getClass().getSimpleName() );
        }
        return (LabelSchemaDescriptor)schemaDescriptor;
    }

    private static MultiTokenSchemaDescriptor readNonSchemaSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schemaDescriptor = readNonSchema( source );
        if ( !(schemaDescriptor instanceof MultiTokenSchemaDescriptor) )
        {
            throw new MalformedSchemaRuleException(
                    "Non schema IndexRules must have MultiTokenSchemaDescriptor, got " + schemaDescriptor.getClass().getSimpleName() );
        }
        return (MultiTokenSchemaDescriptor) schemaDescriptor;
    }

    private static IndexProvider.Descriptor readIndexProviderDescriptor( ByteBuffer source )
    {
        String providerKey = getDecodedStringFrom( source );
        String providerVersion = getDecodedStringFrom( source );
        return new IndexProvider.Descriptor( providerKey, providerVersion );
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

    private static String readMetaData( ByteBuffer source )
    {
        String metadata = "";
        if ( source.remaining() >= UTF8.MINIMUM_SERIALISED_LENGTH_BYTES )
        {
            metadata = UTF8.getDecodedStringFrom( source );
        }
        return metadata;
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
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown schema descriptor type '%d'.",
                    schemaDescriptorType ) );
        }
    }

    private static SchemaDescriptor readNonSchema( ByteBuffer source ) throws MalformedSchemaRuleException
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

    /**
     * Serialize the provided IndexRule onto the target buffer
     * @param indexRule the IndexRule to serialize
     * @throws IllegalStateException if the IndexRule is of type unique, but the owning constrain has not been set
     */
    public static byte[] serialize( IndexRule indexRule )
    {
        ByteBuffer target = ByteBuffer.allocate( lengthOf( indexRule ) );
        target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
        target.put( INDEX_RULE );

        IndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
        UTF8.putEncodedStringInto( providerDescriptor.getKey(), target );
        UTF8.putEncodedStringInto( providerDescriptor.getVersion(), target );

        Type type = indexRule.type();
        switch ( type )
        {
        case GENERAL:
            target.put( GENERAL_INDEX );
            break;

        case UNIQUE:
            target.put( UNIQUE_INDEX );

            // The owning constraint can be null. See IndexRule.getOwningConstraint()
            Long owningConstraint = indexRule.getOwningConstraint();
            target.putLong( owningConstraint == null ? NO_OWNING_CONSTRAINT_YET : owningConstraint );
            break;

        case NON_SCHEMA:
            target.put( NON_SCHEMA_INDEX );
            break;

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.", type ) );
        }

        indexRule.schema().processWith( new SchemaDescriptorSerializer( target ) );
        UTF8.putEncodedStringInto( indexRule.getName(), target );
        UTF8.putEncodedStringInto( indexRule.getMetadata(), target );
        return target.array();
    }

    // LENGTH OF

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

            int[] propertyIds = schema.getPropertyIds();
            target.putShort( (short)propertyIds.length );
            for ( int propertyId : propertyIds )
            {
                target.putInt( propertyId );
            }
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            target.put( SIMPLE_REL_TYPE );
            target.putInt( schema.getRelTypeId() );

            int[] propertyIds = schema.getPropertyIds();
            target.putShort( (short) propertyIds.length );
            for ( int propertyId : propertyIds )
            {
                target.putInt( propertyId );
            }
        }

        @Override
        public void processSpecific( MultiTokenSchemaDescriptor schema )
        {
            if ( schema.entityType() == EntityType.NODE )
            {
                target.put( SIMPLE_LABEL );
            }
            else
            {
                target.put( SIMPLE_REL_TYPE );
            }

            int[] entityTokenIds = schema.getEntityTokenIds();
            target.putShort( (short) entityTokenIds.length );
            for ( int entityTokenId : entityTokenIds )
            {
                target.putInt( entityTokenId );
            }

            int[] propertyIds = schema.getPropertyIds();
            target.putShort( (short)propertyIds.length );
            for ( int propertyId : propertyIds )
            {
                target.putInt( propertyId );
            }
        }
    }
}
