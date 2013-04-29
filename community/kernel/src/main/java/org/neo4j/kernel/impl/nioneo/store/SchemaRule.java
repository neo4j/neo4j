/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;

import static org.neo4j.helpers.Exceptions.launderedException;

public interface SchemaRule extends RecordSerializable
{
    /**
     * The persistence id for this rule.
     */
    long getId();
    
    /**
     * @return id of label to which this schema rule has been attached
     */
    long getLabel();

    /**
     * @return the kind of this schema rule
     */
    Kind getKind();
    
    public static enum Kind
    {
        INDEX_RULE( 1, IndexRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, long labelId, ByteBuffer buffer )
            {
                return new IndexRule( id, labelId, buffer );
            }
        },
        UNIQUENESS_CONSTRAINT( 2, UniquenessConstraintRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, long labelId, ByteBuffer buffer )
            {
                return new UniquenessConstraintRule( id, labelId, buffer );
            }
        };

        private final byte id;
        private final Class<? extends SchemaRule> ruleClass;

        private Kind( int id, Class<? extends SchemaRule> ruleClass )
        {
            assert id > 0 : "Kind id 0 is reserved";
            this.id = (byte) id;
            this.ruleClass = ruleClass;
        }
        
        public Class<? extends SchemaRule> getRuleClass()
        {
            return this.ruleClass;
        }
        
        public byte id()
        {
            return this.id;
        }
        
        protected abstract SchemaRule newRule( long id, long labelId, ByteBuffer buffer );
        
        public static SchemaRule deserialize( long id, ByteBuffer buffer )
        {
            long labelId = buffer.getInt();
            Kind kind = kindForId( buffer.get() );
            try
            {
                return kind.newRule( id, labelId, buffer );
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
        
        public static Kind kindForId( byte id )
        {
            switch ( id )
            {
            case 1: return INDEX_RULE;
            case 2: return UNIQUENESS_CONSTRAINT;
            default:
                throw new IllegalArgumentException( "Unknown kind id " + id );
            }
        }
    }
}
