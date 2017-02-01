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

import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;
import org.neo4j.kernel.api.schema_new.SchemaProcessor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.schema_new.SchemaUtil.noopTokenNameLookup;
import static org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory.uniqueForLabel;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.CONSTRAINT_INDEX_RULE;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.INDEX_RULE;
import static org.neo4j.string.UTF8.getDecodedStringFrom;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule implements SchemaRule, NewIndexDescriptor.Supplier
{
    static final long NO_OWNING_CONSTRAINT = -1;

    private final long id;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final NewIndexDescriptor descriptor;
    /**
     * Non-null for constraint indexes, equal to {@link #NO_OWNING_CONSTRAINT} for
     * constraint indexes with no owning constraint record.
     */
    private final long owningConstraint;

    public static IndexRule indexRule( long id, NewIndexDescriptor descriptor,
                                       SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, providerDescriptor, descriptor, NO_OWNING_CONSTRAINT );
    }

    public static IndexRule constraintIndexRule( long id, NewIndexDescriptor descriptor,
                                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                                 long owningConstraint )
    {
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint );
    }

    IndexRule( long id, SchemaIndexProvider.Descriptor providerDescriptor,
            NewIndexDescriptor descriptor, long owningConstraint )
    {
        this.id = id;
        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.descriptor = descriptor;
        this.owningConstraint = owningConstraint;
        this.providerDescriptor = providerDescriptor;
    }

    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    public boolean canSupportUniqueConstraint()
    {
        return descriptor.type() == NewIndexDescriptor.Type.UNIQUE;
    }

    public Long getOwningConstraint()
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
        long owningConstraint = this.owningConstraint;
        if ( owningConstraint == NO_OWNING_CONSTRAINT )
        {
            return null;
        }
        return owningConstraint;
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( id, descriptor, providerDescriptor, constraintId );
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public int length()
    {
        return lengthComputer.computeSpecific( descriptor.schema() );
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        new Serializer( target ).processSpecific( descriptor.schema() );
    }

    @Override
    public String toString()
    {
        String ownerString = "";
        if ( canSupportUniqueConstraint() )
        {
            ownerString = ", owner=" + owningConstraint;
        }

        return "IndexRule[id=" + id + ", descriptor=" + descriptor.userDescription( noopTokenNameLookup ) +
               ", provider=" + providerDescriptor + ownerString + "]";
    }

    @Override
    public LabelSchemaDescriptor getSchemaDescriptor()
    {
        return descriptor.schema();
    }

    public NewIndexDescriptor getIndexDescriptor()
    {
        return descriptor;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof IndexRule )
        {
            IndexRule that = (IndexRule) o;
            return this.descriptor.equals( that.descriptor );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.descriptor.hashCode();
    }

    // ------- HELPERS -------

    private SchemaComputer<Integer> lengthComputer =
            new SchemaComputer<Integer>() {
                @Override
                public Integer computeSpecific( LabelSchemaDescriptor schema )
                {
                    // regardless of descriptor.type()
                    return    4 /* label id */
                            + 1 /* kind id */
                            + UTF8.computeRequiredByteBufferSize( providerDescriptor.getKey() )
                            + UTF8.computeRequiredByteBufferSize( providerDescriptor.getVersion() )
                            + 2                              /* number of property keys */
                            + 8 * 1                          /* the property keys, for now only 1 */
                            + (canSupportUniqueConstraint() ? 8 : 0)  /* constraint indexes have an owner field */;
                }

                @Override
                public Integer computeSpecific( RelationTypeSchemaDescriptor schema )
                {
                    throw new UnsupportedOperationException( "This constraint type is not yet supported by the store" );
                }
            };

    class Serializer implements SchemaProcessor
    {
        private final ByteBuffer buffer;

        Serializer( ByteBuffer buffer )
        {
            this.buffer = buffer;
        }

        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            // regardless of descriptor.type()
            buffer.putInt( schema.getLabelId() );
            Kind kind = canSupportUniqueConstraint() ? CONSTRAINT_INDEX_RULE : INDEX_RULE;
            buffer.put( kind.id() );
            UTF8.putEncodedStringInto( providerDescriptor.getKey(), buffer );
            UTF8.putEncodedStringInto( providerDescriptor.getVersion(), buffer );
            buffer.putShort( (short) 1 /*propertyKeys.length*/ );
            buffer.putLong( schema.getPropertyIds()[0] );
            if ( canSupportUniqueConstraint() )
            {
                buffer.putLong( owningConstraint );
            }
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( "This index type is not yet supported by the store" );
        }
    }
}
