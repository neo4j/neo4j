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
import java.util.Optional;

import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaProcessor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;

import static org.neo4j.kernel.api.schema_new.SchemaUtil.noopTokenNameLookup;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.NODE_PROPERTY_EXISTENCE_CONSTRAINT;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.UNIQUENESS_CONSTRAINT;

public class ConstraintRule extends AbstractSchemaRule implements ConstraintDescriptor.Supplier
{
    private final Optional<Long> ownedIndexRule;
    private final ConstraintDescriptor descriptor;

    public static ConstraintRule readUniquenessConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, readPropertyKeys( buffer ) ),
                readOwnedIndexRule( buffer ) );
    }

    public static ConstraintRule readNodePropertyExistenceConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.existsForLabel( labelId, readPropertyKey( buffer ) ),
                Optional.empty() );
    }

    public static ConstraintRule readRelPropertyExistenceConstraintRule( long id, int relTypeId, ByteBuffer buffer )
    {
        return new ConstraintRule( id,
                ConstraintDescriptorFactory.existsForRelType( relTypeId, readPropertyKey( buffer ) ),
                Optional.empty() );
    }

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor )
    {
        return new ConstraintRule( id, descriptor, Optional.empty() );
    }

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor, long ownedIndexRule )
    {
        return new ConstraintRule( id, descriptor, Optional.of( ownedIndexRule ) );
    }

    private static int readPropertyKey( ByteBuffer buffer )
    {
        return buffer.getInt();
    }

    private static int[] readPropertyKeys( ByteBuffer buffer )
    {
        int[] keys = new int[buffer.get()];
        for ( int i = 0; i < keys.length; i++ )
        {
            keys[i] = safeCastLongToInt( buffer.getLong() );
        }
        return keys;
    }

    private static Optional<Long> readOwnedIndexRule( ByteBuffer buffer )
    {
        return Optional.of( buffer.getLong() );
    }

    private ConstraintRule( long id, ConstraintDescriptor descriptor, Optional<Long> ownedIndexRule )
    {
        super( id );
        this.descriptor = descriptor;
        this.ownedIndexRule = ownedIndexRule;
    }

    @Override
    public String toString()
    {
        return "ConstraintRule[id=" + id + ", descriptor=" + descriptor.userDescription( noopTokenNameLookup ) + ", " +
                "ownedIndex=" + ownedIndexRule + "]";
    }

    @Override
    public SchemaDescriptor getSchemaDescriptor()
    {
        return descriptor.schema();
    }

    public ConstraintDescriptor getConstraintDescriptor()
    {
        return descriptor;
    }

    public long getOwnedIndex()
    {
        return ownedIndexRule.orElseThrow( IllegalStateException::new );
    }

    @Override
    public int length()
    {
        return descriptor.schema().compute( lengthComputer );
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        new Serializer( target ).process( descriptor.schema() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof ConstraintRule )
        {
            ConstraintRule that = (ConstraintRule) o;
            return this.descriptor.equals( that.descriptor );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return descriptor.hashCode();
    }

    private SchemaComputer<Integer> lengthComputer =
            new SchemaComputer<Integer>() {
                @Override
                public Integer compute( LabelSchemaDescriptor schema )
                {
                    switch ( descriptor.type() )
                    {
                    case UNIQUE:
                        return  4 + /* label */
                                1 + /* kind id */
                                1 + /* the number of properties that form a unique tuple */
                                8 * schema.getPropertyIds().length + /* the property keys themselves */
                                8;  /* owned index rule */

                    case EXISTS:
                        return  4 /* label id */ +
                                1 /* kind id */ +
                                4; /* property key id */

                    default:
                        throw new UnsupportedOperationException( "This constraint type is not yet supported by the store" );
                    }
                }

                @Override
                public Integer compute( RelationTypeSchemaDescriptor schema )
                {
                    return  4 /* relationship type id */ +
                            1 /* kind id */ +
                            4; /* property key id */
                }
            };

    class Serializer implements SchemaProcessor {

        private final ByteBuffer buffer;

        Serializer( ByteBuffer buffer )
        {
            this.buffer = buffer;
        }

        @Override
        public void process( LabelSchemaDescriptor schema )
        {
            switch ( descriptor.type() )
            {
            case UNIQUE:
                buffer.putInt( schema.getLabelId() );
                buffer.put( UNIQUENESS_CONSTRAINT.id() );
                buffer.put( (byte) schema.getPropertyIds().length );
                for ( int propertyKeyId : schema.getPropertyIds() )
                {
                    buffer.putLong( propertyKeyId );
                }
                if ( ownedIndexRule.isPresent() )
                {
                    buffer.putLong( ownedIndexRule.get() );
                }
                break;

            case EXISTS:
                buffer.putInt( schema.getLabelId() );
                buffer.put( NODE_PROPERTY_EXISTENCE_CONSTRAINT.id() );
                buffer.putInt( schema.getPropertyIds()[0] ); // only one property supported by store format atm
                break;

            default:
                throw new UnsupportedOperationException( "This constraint type is not yet supported by the store" );
            }
        }

        @Override
        public void process( RelationTypeSchemaDescriptor schema )
        {
            buffer.putInt( schema.getRelTypeId() );
            buffer.put( RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT.id() );
            buffer.putInt( schema.getPropertyIds()[0] ); // only one property supported by store format atm
        }
    }
}
