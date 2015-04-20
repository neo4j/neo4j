/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

import static org.neo4j.helpers.UTF8.getDecodedStringFrom;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends AbstractSchemaRule
{
    private static final long NO_OWNING_CONSTRAINT = -1;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final int propertyKey;
    /**
     * Non-null for constraint indexes, equal to {@link #NO_OWNING_CONSTRAINT} for
     * constraint indexes with no owning constraint record.
     */
    private final Long owningConstraint;

    static IndexRule readIndexRule( long id, boolean constraintIndex, int label, ByteBuffer serialized )
    {
        SchemaIndexProvider.Descriptor providerDescriptor = readProviderDescriptor( serialized );
        int propertyKeyId = readPropertyKey( serialized );
        if ( constraintIndex )
        {
            long owningConstraint = readOwningConstraint( serialized );
            return constraintIndexRule( id, label, propertyKeyId, providerDescriptor, owningConstraint );
        }
        else
        {
            return indexRule( id, label, propertyKeyId, providerDescriptor );
        }
    }

    public static IndexRule indexRule( long id, int label, int propertyKeyId,
                                       SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, label, propertyKeyId, providerDescriptor, null );
    }

    public static IndexRule constraintIndexRule( long id, int label, int propertyKeyId,
                                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, label, propertyKeyId, providerDescriptor,
                              owningConstraint == null ? NO_OWNING_CONSTRAINT : owningConstraint );
    }

    public IndexRule( long id, int label, int propertyKey, SchemaIndexProvider.Descriptor providerDescriptor,
                       Long owningConstraint )
    {
        super( id, label, indexKind( owningConstraint ) );
        this.owningConstraint = owningConstraint;

        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.providerDescriptor = providerDescriptor;
        this.propertyKey = propertyKey;
    }

    private static Kind indexKind( Long owningConstraint )
    {
        return owningConstraint == null ? Kind.INDEX_RULE : Kind.CONSTRAINT_INDEX_RULE;
    }

    private static SchemaIndexProvider.Descriptor readProviderDescriptor( ByteBuffer serialized )
    {
        String providerKey = getDecodedStringFrom( serialized );
        String providerVersion = getDecodedStringFrom( serialized );
        return new SchemaIndexProvider.Descriptor( providerKey, providerVersion );
    }

    private static int readPropertyKey( ByteBuffer serialized )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = serialized.getShort();
        assert count == 1;

        // Changed from being a long to an int 2013-09-10, but keeps reading a long to not change the store format.
        return safeCastLongToInt( serialized.getLong() );
    }

    private static long readOwningConstraint( ByteBuffer serialized )
    {
        return serialized.getLong();
    }

    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    public int getPropertyKey()
    {
        return propertyKey;
    }

    public boolean isConstraintIndex()
    {
        return owningConstraint != null;
    }

    public Long getOwningConstraint()
    {
        if ( !isConstraintIndex() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
        if ( owningConstraint == NO_OWNING_CONSTRAINT )
        {
            return null;
        }
        return owningConstraint;
    }

    @Override
    public int length()
    {
        return super.length()
               + UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() )
               + UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() )
               + 2 * 1                              /* number of property keys, for now always 1 */
               + 8                                  /* the property keys */
               + (isConstraintIndex() ? 8 : 0)      /* constraint indexes have an owner field */;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        super.serialize( target );
        UTF8.putEncodedStringInto( providerDescriptor.getKey(), target );
        UTF8.putEncodedStringInto( providerDescriptor.getVersion(), target );
        target.putShort( (short) 1 /*propertyKeys.length*/ );
        target.putLong( propertyKey );
        if ( isConstraintIndex() )
        {
            target.putLong( owningConstraint );
        }
    }

    @Override
    public int hashCode()
    {
        // TODO: Think if this needs to be extended with providerDescriptor
        return 31 * super.hashCode() + propertyKey;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( !super.equals( obj ) )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        IndexRule other = (IndexRule) obj;
        return propertyKey == other.propertyKey;
    }

    @Override
    protected String innerToString()
    {
        StringBuilder result = new StringBuilder( ", provider=" ).append( providerDescriptor ).append( ", properties=" )
                                                                 .append( propertyKey );
        if ( owningConstraint != null )
        {
            result.append( ", owner=" );
            if ( owningConstraint == -1 )
            {
                result.append( "<not set>" );
            }
            else
            {
                result.append( owningConstraint );
            }
        }
        return result.toString();
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !isConstraintIndex() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( getId(), getLabel(), getPropertyKey(), getProviderDescriptor(), constraintId );
    }
}
