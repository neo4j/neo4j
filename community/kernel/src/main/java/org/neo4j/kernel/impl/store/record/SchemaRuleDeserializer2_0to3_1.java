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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.schema.SchemaRule.Kind;

import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

/**
 * Deserializes SchemaRules from a ByteBuffer.
 */
public class SchemaRuleDeserializer2_0to3_1
{
    private static final Long NO_OWNED_INDEX_RULE = null;
    private static final long NO_OWNING_CONSTRAINT = -1;

    private SchemaRuleDeserializer2_0to3_1()
    {
    }

    static boolean isLegacySchemaRule( byte schemaRuleType )
    {
        return schemaRuleType >= 1 && schemaRuleType <= SchemaRule.Kind.values().length;
    }

    static SchemaRule deserialize( long id, int labelId, byte kindByte, ByteBuffer buffer ) throws
            MalformedSchemaRuleException
    {
        Kind kind = Kind.forId( kindByte );
        try
        {
            SchemaRule rule = newRule( kind, id, labelId, buffer );
            if ( null == rule )
            {
                throw new MalformedSchemaRuleException( null,
                        "Deserialized null schema rule for id %d with kind %s", id, kind.name() );
            }
            return rule;
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( e,
                    "Could not deserialize schema rule for id %d with kind %s", id, kind.name() );
        }
    }

    private static SchemaRule newRule( Kind kind, long id, int labelId, ByteBuffer buffer )
    {
        switch ( kind )
        {
        case INDEX_RULE:
            return readIndexRule( id, false, labelId, buffer );
        case CONSTRAINT_INDEX_RULE:
            return readIndexRule( id, true, labelId, buffer );
        case UNIQUENESS_CONSTRAINT:
            return readUniquenessConstraintRule( id, labelId, buffer );
        case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
            return readNodePropertyExistenceConstraintRule( id, labelId, buffer );
        case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
            return readRelPropertyExistenceConstraintRule( id, labelId, buffer );
        default:
            throw new IllegalArgumentException( kind.name() );
        }
    }

    // === INDEX RULES ===

    private static IndexRule readIndexRule( long id, boolean constraintIndex, int label, ByteBuffer serialized )
    {
        IndexProvider.Descriptor providerDescriptor = readIndexProviderDescriptor( serialized );
        int[] propertyKeyIds = readIndexPropertyKeys( serialized );
        SchemaIndexDescriptor descriptor = constraintIndex ?
                                           SchemaIndexDescriptorFactory.uniqueForLabel( label, propertyKeyIds ) :
                                           SchemaIndexDescriptorFactory.forLabel( label, propertyKeyIds );
        long owningConstraint = constraintIndex ? readOwningConstraint( serialized ) : NO_OWNING_CONSTRAINT;
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint );
    }

    private static IndexProvider.Descriptor readIndexProviderDescriptor( ByteBuffer serialized )
    {
        String providerKey = getDecodedStringFrom( serialized );
        String providerVersion = getDecodedStringFrom( serialized );
        return new IndexProvider.Descriptor( providerKey, providerVersion );
    }

    private static int[] readIndexPropertyKeys( ByteBuffer serialized )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = serialized.getShort();
        assert count >= 1;

        // Changed from being a long to an int 2013-09-10, but keeps reading a long to not change the store format.
        int[] props = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            props[i] = safeCastLongToInt( serialized.getLong() );
        }
        return props;
    }

    private static long readOwningConstraint( ByteBuffer serialized )
    {
        return serialized.getLong();
    }

    // === CONSTRAINT RULES ===

    public static ConstraintRule readUniquenessConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, readConstraintPropertyKeys( buffer ) ),
                readOwnedIndexRule( buffer ) );
    }

    public static ConstraintRule readNodePropertyExistenceConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.existsForLabel( labelId, readPropertyKey( buffer ) ),
                NO_OWNED_INDEX_RULE );
    }

    public static ConstraintRule readRelPropertyExistenceConstraintRule( long id, int relTypeId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.existsForRelType( relTypeId, readPropertyKey( buffer ) ),
                NO_OWNED_INDEX_RULE );
    }

    private static int readPropertyKey( ByteBuffer buffer )
    {
        return buffer.getInt();
    }

    private static int[] readConstraintPropertyKeys( ByteBuffer buffer )
    {
        int[] keys = new int[buffer.get()];
        for ( int i = 0; i < keys.length; i++ )
        {
            keys[i] = safeCastLongToInt( buffer.getLong() );
        }
        return keys;
    }

    private static Long readOwnedIndexRule( ByteBuffer buffer )
    {
        return buffer.getLong();
    }
}
