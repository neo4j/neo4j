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
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.SchemaProcessor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;

import static java.lang.String.format;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

public class SchemaRuleSerialization
{
    // Schema rule type
    private final static byte INDEX_RULE = 1, CONSTRAINT_RULE = 2;

    // Index type
    private final static byte GENERAL_INDEX = 51, UNIQUE_INDEX = 52;

    // Schema type
    private final static byte SIMPLE_LABEL = 101, SIMPLE_REL_TYPE = 102;

    private SchemaRuleSerialization()
    {
    }

    public static SchemaRule deserialize( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        byte schemaRuleType = source.get();
        switch ( schemaRuleType )
        {
        case INDEX_RULE:
            return readIndexRule( id, source );
        default:
            throw new UnsupportedOperationException( format( "Got unknown schema rule type '%d'.", schemaRuleType ) );
        }
    }

    private static IndexRule readIndexRule( long id, ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaIndexProvider.Descriptor indexProvider = readIndexProviderDescriptor( source );
        byte indexRuleType = source.get();
        switch ( indexRuleType )
        {
        case GENERAL_INDEX:
            return IndexRule.indexRule(
                    id,
                    NewIndexDescriptorFactory.forSchema( readLabelSchema( source ) ),
                    indexProvider
                );

        case UNIQUE_INDEX:
            long owningConstraint = source.getLong();
            return IndexRule.constraintIndexRule(
                    id,
                    NewIndexDescriptorFactory.uniqueForSchema( readLabelSchema( source ) ),
                    indexProvider,
                    owningConstraint
                );

        default:
            throw new UnsupportedOperationException( format( "Got unknown index rule type '%d'.", indexRuleType ) );
        }
    }

    private static LabelSchemaDescriptor readLabelSchema( ByteBuffer source ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schemaDescriptor = readSchema( source );
        if ( !(schemaDescriptor instanceof LabelSchemaDescriptor) )
        {
            throw new MalformedSchemaRuleException( "IndexRules must have LabelSchemaDescriptors, got " +
                    ""+schemaDescriptor.getClass().getSimpleName() );
        }
        return (LabelSchemaDescriptor)schemaDescriptor;
    }

    private static SchemaDescriptor readSchema( ByteBuffer source )
    {
        byte schemaDescriptorType = source.get();
        switch ( schemaDescriptorType )
        {
        case SIMPLE_LABEL:
            return SchemaDescriptorFactory.forLabel( source.getInt(), readPropertyIds( source ) );
        default:
            throw new UnsupportedOperationException( format( "Got unknown schema descriptor type '%d'.",
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

    private static SchemaIndexProvider.Descriptor readIndexProviderDescriptor( ByteBuffer source )
    {
        String providerKey = getDecodedStringFrom( source );
        String providerVersion = getDecodedStringFrom( source );
        return new SchemaIndexProvider.Descriptor( providerKey, providerVersion );
    }

    public static void serialize( IndexRule indexRule, ByteBuffer target ) throws MalformedSchemaRuleException
    {
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
            Long owningConstraint = indexRule.getOwningConstraint();
            if ( owningConstraint == null )
            {
                throw new MalformedSchemaRuleException( "Cannot serialize Unique Index before the owning constraint is set." );
            }
            target.putLong( owningConstraint );
            break;

        default:
            throw new UnsupportedOperationException( format( "Got unknown index descriptor type '%s'.",
                    indexDescriptor.type() ) );
        }

        indexDescriptor.schema().processWith( new SchemaDescriptorSerializer( target ) );
    }

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
}
