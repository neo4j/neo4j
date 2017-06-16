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
import java.util.Iterator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.AnyValues;
import org.neo4j.values.TextValue;
import org.neo4j.values.VirtualValue;

import static org.neo4j.values.virtual.ArrayHelpers.asIterator;
import static org.neo4j.values.virtual.ArrayHelpers.isSortedSet;
import static org.neo4j.values.virtual.VirtualValueGroup.LABEL_SET;

public abstract class LabelSet extends VirtualValue implements Iterable<TextValue>
{
    public abstract int size();

    /**
     * This implementation is assuming that the label set is fairly
     * small, maintains a sorted set of labels.
     */
    static class ArrayBasedLabelSet extends LabelSet
    {
        private final TextValue[] labels;

        ArrayBasedLabelSet( TextValue[] labels )
        {
            assert labels != null;
            this.labels = new TextValue[labels.length];
            System.arraycopy( labels, 0, this.labels, 0, this.labels.length );
            Arrays.sort( this.labels, AnyValues.COMPARATOR );
            assert isSortedSet( this.labels, AnyValues.COMPARATOR );
        }

        @Override
        public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
        {
            writer.beginLabels( labels.length );
            for ( TextValue label : labels )
            {
                label.writeTo( writer );
            }
            writer.endLabels();
        }

        @Override
        public int hash()
        {
            return Arrays.hashCode( labels );
        }

        @Override
        public boolean equals( VirtualValue other )
        {
            if ( !(other instanceof LabelSet) )
            {
                return false;
            }
            if ( labels.length != ((LabelSet) other).size() )
            {
                return false;
            }

            if ( other instanceof ArrayBasedLabelSet )
            {   //fast route
                ArrayBasedLabelSet that = (ArrayBasedLabelSet) other;

                for ( int i = 0; i < labels.length; i++ )
                {
                    if ( !labels[i].equals( that.labels[i] ) )
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {   //slow route
                TextValue[] thatLabels = toSortedLabelArray( (LabelSet) other );
                for ( int i = 0; i < size(); i++ )
                {
                    TextValue label1 = labels[i];
                    TextValue label2 = thatLabels[i];
                    if ( !label1.equals( label2 ) )
                    {
                        return false;
                    }
                }
                return true;
            }
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
                if ( otherSet instanceof ArrayBasedLabelSet )
                {
                    ArrayBasedLabelSet otherArraySet = (ArrayBasedLabelSet) otherSet;
                    for ( int i = 0; i < size(); i++ )
                    {
                        x = comparator.compare( labels[i], otherArraySet.labels[i] );
                        if ( x != 0 )
                        {
                            return x;
                        }
                    }
                }
                else
                {
                    TextValue[] thatLabels = toSortedLabelArray( (LabelSet) other );
                    for ( int i = 0; i < size(); i++ )
                    {
                        TextValue label1 = labels[i];
                        TextValue label2 = thatLabels[i];
                        x = comparator.compare( label1, label2 );
                        if ( x != 0 )
                        {
                            return x;
                        }
                    }
                }
            }

            return x;
        }

        @Override
        public String toString()
        {
            return Arrays.toString( labels );
        }

        @Override
        public int size()
        {
            return labels.length;
        }

        @SuppressWarnings( "NullableProblems" )
        @Override
        public Iterator<TextValue> iterator()
        {
            return asIterator( labels );
        }

        private TextValue[] toSortedLabelArray( LabelSet set )
        {
            Iterator<TextValue> thatIterator = set.iterator();
            TextValue[] labelArray = new TextValue[set.size()];
            int i = 0;
            while ( thatIterator.hasNext() )
            {
                labelArray[i++] = thatIterator.next();
            }
            Arrays.sort( labelArray, AnyValues.COMPARATOR );

            return labelArray;
        }
    }
}
