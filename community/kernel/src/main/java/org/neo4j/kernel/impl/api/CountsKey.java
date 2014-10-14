/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.store.counts.CountsRecordType;
import org.neo4j.register.Register;

import static org.neo4j.kernel.impl.store.counts.CountsRecordType.INDEX;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.NODE;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.RELATIONSHIP;

public abstract class CountsKey implements Comparable<CountsKey>
{
    public static NodeKey nodeKey( int labelId )
    {
        return new NodeKey( labelId );
    }

    public static RelationshipKey relationshipKey( int startLabelId, int typeId, int endLabelId )
    {
        return new RelationshipKey( startLabelId, typeId, endLabelId );
    }

    public static IndexKey indexKey( int labelId, int propertyKeyId )
    {
        return new IndexKey( labelId, propertyKeyId );
    }

    private CountsKey()
    {
        // all subclasses are internal
    }

    public abstract CountsRecordType recordType();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals( Object obj );

    @Override
    public abstract String toString();

    public abstract void accept( CountsVisitor visitor, Register.LongRegister count );

    public static final class NodeKey extends CountsKey
    {
        private final int labelId;

        private NodeKey( int labelId )
        {
            this.labelId = labelId;
        }

        public int labelId()
        {
            return labelId;
        }

        @Override
        public String toString()
        {
            return String.format( "CountsKey[(%s)]", label( labelId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.LongRegister count )
        {
            visitor.visitNodeCount( labelId, count.read() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return NODE;
        }

        @Override
        public int hashCode()
        {
            return labelId;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o || (o instanceof NodeKey) && labelId == ((NodeKey) o).labelId;
        }

        @Override
        public int compareTo( CountsKey other )
        {
            if ( other instanceof NodeKey )
            {
                NodeKey that = (NodeKey) other;
                return this.labelId - that.labelId;
            }
            else
            {
                return recordType().ordinal() - other.recordType().ordinal();
            }
        }
    }

    public static final class RelationshipKey extends CountsKey
    {
        private final int startLabelId;
        private final int typeId;
        private final int endLabelId;

        private RelationshipKey( int startLabelId, int typeId, int endLabelId )
        {
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
        }

        public int startLabelId()
        {
            return startLabelId;
        }

        public int typeId()
        {
            return typeId;
        }

        public int endLabelId()
        {
            return endLabelId;
        }

        @Override
        public String toString()
        {
            return String.format( "CountsKey[(%s)-%s->(%s)]",
                                  label( startLabelId ), relationshipType( typeId ), label( endLabelId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.LongRegister count )
        {
            visitor.visitRelationshipCount( startLabelId, typeId, endLabelId, count.read() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return RELATIONSHIP;
        }

        @Override
        public int hashCode()
        {
            int result = startLabelId;
            result = 31 * result + typeId;
            result = 31 * result + endLabelId;
            return result;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( (o instanceof RelationshipKey) )
            {
                RelationshipKey that = (RelationshipKey) o;
                return endLabelId == that.endLabelId && startLabelId == that.startLabelId && typeId == that.typeId;
            }
            return false;
        }

        @Override
        public int compareTo( CountsKey o )
        {
            if ( o instanceof RelationshipKey )
            {
                RelationshipKey that = (RelationshipKey) o;
                if ( this.typeId != that.typeId )
                {
                    return this.typeId - that.typeId;
                }
                if ( this.startLabelId != that.startLabelId )
                {
                    return this.startLabelId - that.startLabelId;
                }
                return this.endLabelId - that.endLabelId;
            }
            else
            {
                return recordType().ordinal() - o.recordType().ordinal();
            }
        }
    }

    public static final class IndexKey extends CountsKey
    {
        private final int labelId;
        private final int propertyKeyId;

        private IndexKey( int labelId, int propertyKeyId )
        {
            this.labelId = labelId;
            this.propertyKeyId = propertyKeyId;
        }

        public int labelId()
        {
            return labelId;
        }

        public int getPropertyKeyId()
        {
            return propertyKeyId;
        }

        @Override
        public String toString()
        {
            return String.format( "CountsKey[(%s {%s})", label( labelId ), propertyKey( propertyKeyId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.LongRegister count )
        {
            visitor.visitIndexCount( labelId, propertyKeyId, count.read() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return INDEX;
        }

        @Override
        public int hashCode()
        {
            int result = labelId;
            result = 31 * result + propertyKeyId;
            return result;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            IndexKey indexKey = (IndexKey) o;
            return labelId == indexKey.labelId && propertyKeyId == indexKey.propertyKeyId;

        }

        @Override
        public int compareTo( CountsKey other )
        {
            if ( other instanceof IndexKey )
            {
                IndexKey that = (IndexKey) other;
                int cmp = this.labelId - that.labelId;
                if ( cmp == 0 )
                {
                    cmp = this.propertyKeyId - that.propertyKeyId;
                }
                return cmp;
            }
            else
            {
                return recordType().ordinal() - other.recordType().ordinal();
            }
        }
    }

    public static String label( int id )
    {
        return id == ReadOperations.ANY_LABEL ? "" : (":label=" + id);
    }

    public static String propertyKey( int id )
    {
        return id == ReadOperations.NO_SUCH_PROPERTY_KEY ? "" : (":propertyKey=" + id);
    }

    public static String relationshipType( int id )
    {
        return id == ReadOperations.ANY_RELATIONSHIP_TYPE ? "" : ("[:type=" + id + "]");
    }
}
