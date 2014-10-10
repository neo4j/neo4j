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
import org.neo4j.register.Register;

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

    public static IndexKey indexKey( int indexId )
    {
        return new IndexKey( indexId );
    }

    private CountsKey()
    {
        // all subclasses are internal
    }

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
        public boolean equals( Object o )
        {
            return this == o || (o instanceof NodeKey) && labelId == ((NodeKey) o).labelId;
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
        public int hashCode()
        {
            return labelId;
        }

        @Override
        public int compareTo( CountsKey o )
        {
            if ( o instanceof NodeKey )
            {
                NodeKey that = (NodeKey) o;
                return this.labelId - that.labelId;
            }
            else
            {
                return -1;
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
        public int hashCode()
        {
            int result = startLabelId;
            result = 31 * result + typeId;
            result = 31 * result + endLabelId;
            return result;
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
                return 1;
            }
        }
    }

    public static final class IndexKey extends CountsKey
    {
        private final int indexId;

        private IndexKey( int indexId )
        {
            this.indexId = indexId;
        }

        public int indexId()
        {
            return indexId;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o || (o instanceof IndexKey) && indexId == ((IndexKey) o).indexId;
        }

        @Override
        public String toString()
        {
            return String.format( "CountsKey[(:index=%s)]", indexId );
        }

        @Override
        public void accept( CountsVisitor visitor, Register.LongRegister count )
        {
            visitor.visitIndexCount( indexId, count.read() );
        }

        @Override
        public int hashCode()
        {
            return indexId;
        }

        @Override
        public int compareTo( CountsKey o )
        {
            if ( o instanceof IndexKey )
            {
                IndexKey that = (IndexKey) o;
                return this.indexId - that.indexId;
            }
            else
            {
                return -1;
            }
        }
    }

    public static String label( int id )
    {
        return id == ReadOperations.ANY_LABEL ? "" : (":label=" + id);
    }

    public static String relationshipType( int id )
    {
        return id == ReadOperations.ANY_RELATIONSHIP_TYPE ? "" : ("[:type=" + id + "]");
    }
}
