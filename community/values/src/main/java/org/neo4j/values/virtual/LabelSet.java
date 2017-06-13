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
package org.neo4j.values.virtual;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

import static org.neo4j.values.virtual.ArrayHelpers.isSortedSet;
import static org.neo4j.values.virtual.VirtualValueGroup.LABEL_SET;

abstract class LabelSet extends VirtualValue
{
    public abstract int size();

    public abstract int getLabelId( int offset );

    static class ArrayBasedLabelSet extends LabelSet
    {
        private final int[] labelIds;

        ArrayBasedLabelSet( int[] labelIds )
        {
            assert labelIds != null;
            assert isSortedSet( labelIds );

            this.labelIds = labelIds;
        }

        @Override
        public int size()
        {
            return labelIds.length;
        }

        @Override
        public int getLabelId( int offset )
        {
            return labelIds[offset];
        }

        @Override
        public void writeTo( AnyValueWriter writer )
        {
            writer.beginLabels( labelIds.length );
            for ( int labelId : labelIds )
            {
                writer.writeLabel( labelId );
            }
            writer.endLabels();
        }

        @Override
        public int hash()
        {
            return Arrays.hashCode( labelIds );
        }

        @Override
        public boolean equals( VirtualValue other )
        {
            if ( !(other instanceof LabelSet) )
            {
                return false;
            }
            LabelSet that = (LabelSet) other;
            if ( labelIds.length != that.size() )
            {
                return false;
            }
            for ( int i = 0; i < labelIds.length; i++ )
            {
                if ( labelIds[i] != that.getLabelId( i ) )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public VirtualValueGroup valueGroup()
        {
            return LABEL_SET;
        }

        @Override
        public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
        {
            if ( !(other instanceof LabelSet) )
            {
                throw new IllegalArgumentException( "Cannot compare different virtual values" );
            }

            LabelSet otherSet = (LabelSet) other;
            int x = Integer.compare( this.size(), otherSet.size() );

            if ( x == 0 )
            {
                for ( int i = 0; i < size(); i++ )
                {
                    x = Integer.compare( this.labelIds[i], otherSet.getLabelId( i ) );
                    if ( x != 0 )
                    {
                        return x;
                    }
                }
            }

            return x;
        }
    }
}
