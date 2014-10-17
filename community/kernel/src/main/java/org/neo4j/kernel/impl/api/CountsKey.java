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

import static org.neo4j.kernel.impl.store.counts.CountsRecordType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.CountsRecordType.INDEX_SIZE;

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

    public static IndexKey indexSizeKey( int labelId, int propertyKeyId )
    {
        return new IndexSizeKey( labelId, propertyKeyId );
    }

    public static IndexKey indexSampleKey( int labelId, int propertyKeyId )
    {
        return new IndexSampleKey( labelId, propertyKeyId );
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

    public abstract void accept( CountsVisitor visitor, Register.DoubleLongRegister count );

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
            return String.format( "NodeKey[(%s)]", label( labelId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.DoubleLongRegister count )
        {
            visitor.visitNodeCount( labelId, count.readSecond() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return ENTITY_NODE;
        }

        @Override
        public int hashCode()
        {
            int result = labelId;
            result = 31 * result + recordType().hashCode();
            return result;
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
            return String.format( "RelationshipKey[(%s)-%s->(%s)]",
                                  label( startLabelId ), relationshipType( typeId ), label( endLabelId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.DoubleLongRegister count )
        {
            visitor.visitRelationshipCount( startLabelId, typeId, endLabelId, count.readSecond() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return ENTITY_RELATIONSHIP;
        }

        @Override
        public int hashCode()
        {
            int result = startLabelId;
            result = 31 * result + typeId;
            result = 31 * result + endLabelId;
            result = 31 * result + recordType().hashCode();
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
        public int compareTo( CountsKey other )
        {
            if ( other instanceof RelationshipKey )
            {
                RelationshipKey that = (RelationshipKey) other;
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
                return recordType().ordinal() - other.recordType().ordinal();
            }
        }
    }

    public static abstract class IndexKey extends CountsKey
    {
        private final int labelId;
        private final int propertyKeyId;
        private final CountsRecordType type;

        private IndexKey( int labelId, int propertyKeyId, CountsRecordType type )
        {
            this.labelId = labelId;
            this.propertyKeyId = propertyKeyId;
            this.type = type;
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
            return String.format( "IndexKey[%s (%s {%s})]", type.name(), label( labelId ), propertyKey( propertyKeyId ) );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.DoubleLongRegister count )
        {
            visitor.visitIndexSizeCount( labelId, propertyKeyId, count.readSecond() );
        }

        @Override
        public CountsRecordType recordType()
        {
            return type;
        }


        @Override
        public int hashCode()
        {
            int result = labelId;
            result = 31 * result + propertyKeyId;
            result = 31 * result + type.hashCode();
            return result;
        }

        @Override
        public boolean equals( Object other )
        {
            if ( this == other )
            {
                return true;
            }
            if ( other == null || getClass() != other.getClass() )
            {
                return false;
            }

            IndexKey indexKey = (IndexKey) other;
            return labelId == indexKey.labelId && propertyKeyId == indexKey.propertyKeyId && type == indexKey.type;

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

    public final static class IndexSizeKey extends IndexKey
    {
        public IndexSizeKey( int labelId, int propertyKeyId )
        {
            super( labelId, propertyKeyId, INDEX_SIZE );
        }
    }

    public final static class IndexSampleKey extends IndexKey
    {
        public IndexSampleKey( int labelId, int propertyKeyId )
        {
            super( labelId, propertyKeyId, INDEX_SAMPLE );
        }
    }
}
