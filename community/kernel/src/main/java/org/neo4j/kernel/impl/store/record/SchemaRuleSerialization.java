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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.SchemaProcessor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;

import static java.lang.String.format;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

public class SchemaRuleSerialization
{
    // Schema rule type
    // Legacy schema store reserves 1,2,3,4 and 5
    private static final byte INDEX_RULE = 11, CONSTRAINT_RULE = 12;

    // Index type
    private static final byte GENERAL_INDEX = 31, UNIQUE_INDEX = 32;

    // Constraint type
    private static final byte EXISTS_CONSTRAINT = 61, UNIQUE_CONSTRAINT = 62;

    // Schema type
    private static final byte SIMPLE_LABEL = 91, SIMPLE_REL_TYPE = 92;

    private static final long NO_OWNING_CONSTRAINT_YET = -1;
    private static final int LEGACY_LABEL_OR_REL_TYPE_ID = -1;

    private SchemaRuleSerialization()
    {
    }

    // PUBLIC

    /**
     * Parse a SchemaRule from the provided buffer.
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
     * @param indexRule the IndexRule to serialize
     * @param target the target buffer
     * @throws IllegalStateException if the IndexRule is of type unique, but the owning constrain has not been set
     */
    public static void serialize( IndexRule indexRule, ByteBuffer target )
    {
        target.putInt( LEGACY_LABEL_OR_REL_TYPE_ID );
        target.put( INDEX_RULE );

        SchemaIndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
        UTF8.putEncodedStringInto( providerDescriptor.getKey(), target );
        UTF8.putEncodedStringInto( providerDescriptor.getVersion(), target );

        NewIndexDescriptor indexDescriptor = indexRule.getIndexDescriptor();
        switch ( indexDescriptor.type() )
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

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.",
                    indexDescriptor.type() ) );
        }

        indexDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
    }

    /**
     * Serialize the provided ConstraintRule onto the target buffer
     * @param constraintRule the ConstraintRule to serialize
     * @param target the target buffer
     * @throws IllegalStateException if the ConstraintRule is of type unique, but the owned index has not been set
     */
    public static void serialize( ConstraintRule constraintRule, ByteBuffer target )
    {
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

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.",
                    constraintDescriptor.type() ) );
        }

        constraintDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
    }

    /**
     * Compute the byte size needed to serialize the provided IndexRule using serialize.
     * @param indexRule the IndexRule
     * @return the byte size of indexRule
     */
    public static int lengthOf( IndexRule indexRule )
    {
        int length = 4; // legacy label or relType id
        length += 1;    // schema rule type

        SchemaIndexProvider.Descriptor providerDescriptor = indexRule.getProviderDescriptor();
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() );
        length += UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() );

        length += 1; // index type
        NewIndexDescriptor indexDescriptor = indexRule.getIndexDescriptor();
        if ( indexDescriptor.type() == NewIndexDescriptor.Type.UNIQUE )
        {
            length += 8; // owning constraint id
        }

        length += schemaSizeComputer.compute( indexDescriptor.schema() );
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
        if ( constraintDescriptor.type() == ConstraintDescriptor.Type.UNIQUE )
        {
            length += 8; // owned index id
        }

        length += schemaSizeComputer.compute( constraintDescriptor.schema() );
        return length;
    }

    // PRIVATE

    // READ INDEX

    private static IndexRule readIndexRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaIndexProvider.Descriptor indexProvider = readIndexProviderDescriptor( source );
        LabelSchemaDescriptor schema;
        byte indexRuleType = source.get();
        switch ( indexRuleType )
        {
        case GENERAL_INDEX:
            schema = readLabelSchema( source );
            return IndexRule.indexRule(
                    id,
                    NewIndexDescriptorFactory.forSchema( schema ),
                    indexProvider
                );

        case UNIQUE_INDEX:
            long owningConstraint = source.getLong();
            schema = readLabelSchema( source );

            return IndexRule.constraintIndexRule(
                    id,
                    NewIndexDescriptorFactory.uniqueForSchema( schema ),
                    indexProvider,
                    owningConstraint == NO_OWNING_CONSTRAINT_YET ? null : owningConstraint
                );

        default:
            throw new MalformedSchemaRuleException( format( "Got unknown index rule type '%d'.", indexRuleType ) );
        }
    }

    private static LabelSchemaDescriptor readLabelSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schemaDescriptor = readSchema( source );
        if ( !(schemaDescriptor instanceof LabelSchemaDescriptor) )
        {
            throw new MalformedSchemaRuleException( "IndexRules must have LabelSchemaDescriptors, got " +
                    schemaDescriptor.getClass().getSimpleName() );
        }
        return (LabelSchemaDescriptor)schemaDescriptor;
    }

    private static SchemaIndexProvider.Descriptor readIndexProviderDescriptor( ByteBuffer source )
    {
        String providerKey = getDecodedStringFrom( source );
        String providerVersion = getDecodedStringFrom( source );
        return new SchemaIndexProvider.Descriptor( providerKey, providerVersion );
    }

    // READ CONSTRAINT

    private static ConstraintRule readConstraintRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schema;
        byte constraintRuleType = source.get();
        switch ( constraintRuleType )
        {
        case EXISTS_CONSTRAINT:
            schema = readSchema( source );
            return ConstraintRule.constraintRule(
                    id,
                    ConstraintDescriptorFactory.existsForSchema( schema )
            );

        case UNIQUE_CONSTRAINT:
            long ownedIndex = source.getLong();
            schema = readSchema( source );
            return ConstraintRule.constraintRule(
                    id,
                    ConstraintDescriptorFactory.uniqueForSchema( schema ),
                    ownedIndex
            );

        default:
            throw new MalformedSchemaRuleException( format( "Got unknown constraint rule type '%d'.", constraintRuleType ) );
        }
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
            propertyIds = readPropertyIds( source );
            return SchemaDescriptorFactory.forLabel( labelId, propertyIds );
        case SIMPLE_REL_TYPE:
            int relTypeId = source.getInt();
            propertyIds = readPropertyIds( source );
            return SchemaDescriptorFactory.forRelType( relTypeId, propertyIds );
        default:
            throw new MalformedSchemaRuleException( format( "Got unknown schema descriptor type '%d'.",
                    schemaDescriptorType ) );
        }
    }

    private static int[] readPropertyIds( ByteBuffer source )
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
            target.putShort( (short)propertyIds.length );
            for ( int propertyId : propertyIds )
            {
                target.putInt( propertyId );
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
    };
}
