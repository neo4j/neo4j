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
package org.neo4j.storageengine.api;

import org.apache.commons.lang3.mutable.MutableLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

/**
 * An in-memory single-threaded counts holder useful for modifying and reading counts transaction state.
 */
public class CountsDelta implements CountsAccessor, CountsAccessor.Updater
{
    private static final long DEFAULT_COUNT = 0;
    private final Map<Key,MutableLong> counts = new HashMap<>();

    @Override
    public long nodeCount( int labelId )
    {
        return counts( nodeKey( labelId ) ).longValue();
    }

    @Override
    public void incrementNodeCount( long labelId, long delta )
    {
        counts( nodeKey( labelId ) ).add( delta );
    }

    @Override
    public long relationshipCount( int startLabelId, int typeId, int endLabelId )
    {
        return counts( relationshipKey( startLabelId, typeId, endLabelId ) ).longValue();
    }

    @Override
    public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
    {
        if ( delta != 0 )
        {
            counts( relationshipKey( startLabelId, typeId, endLabelId ) ).add( delta );
        }
    }

    @Override
    public void close()
    {
        // this is close() of CountsAccessor.Updater - do nothing.
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        for ( Map.Entry<Key, MutableLong> entry : counts.entrySet() )
        {
            MutableLong register = entry.getValue();
            entry.getKey().accept( visitor, register.longValue() );
        }
    }

    public boolean hasChanges()
    {
        return !counts.isEmpty();
    }

    public List<Difference> verify( CountsVisitor.Visitable visitable )
    {
        Verifier verifier = new Verifier( counts );
        visitable.accept( verifier );
        return verifier.differences();
    }

    public static final class Difference
    {
        private final Key key;
        private final long expectedCount;
        private final long actualCount;

        public Difference( Key key, long expectedCount, long actualCount )
        {
            this.expectedCount = expectedCount;
            this.actualCount = actualCount;
            this.key = requireNonNull( key, "key" );
        }

        @Override
        public String toString()
        {
            return String.format( "%s[%s expected=%d, actual=%d]", getClass().getSimpleName(), key, expectedCount, actualCount );
        }

        public Key key()
        {
            return key;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj instanceof Difference )
            {
                Difference that = (Difference) obj;
                return actualCount == that.actualCount && expectedCount == that.expectedCount && key.equals( that.key );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            int result = key.hashCode();
            result = 31 * result + (int) (expectedCount ^ (expectedCount >>> 32));
            result = 31 * result + (int) (actualCount ^ (actualCount >>> 32));
            return result;
        }
    }

    public void addNode( long[] labels )
    {
        incrementNodeCount( ANY_LABEL, 1 );
        for ( long label : labels )
        {
            incrementNodeCount( (int) label, 1 );
        }
    }

    public void addRelationship( long[] startLabels, int type, long[] endLabels )
    {
        incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
        incrementRelationshipCount( ANY_LABEL, type, ANY_LABEL, 1 );
        for ( long startLabelId : startLabels )
        {
            incrementRelationshipCount( (int) startLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, 1 );
            incrementRelationshipCount( (int) startLabelId, type, ANY_LABEL, 1 );
        }
        for ( long endLabelId : endLabels )
        {
            incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, (int) endLabelId, 1 );
            incrementRelationshipCount( ANY_LABEL, type, (int) endLabelId, 1 );
        }
    }

    private MutableLong counts( Key key )
    {
        return counts.computeIfAbsent( key, k -> new MutableLong( DEFAULT_COUNT ) );
    }

    private static class Verifier implements CountsVisitor
    {
        private final Map<Key, MutableLong> counts;
        private final List<Difference> differences = new ArrayList<>();

        Verifier( Map<Key, MutableLong> counts )
        {
            this.counts = new HashMap<>( counts );
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            verify( nodeKey( labelId ), count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            verify( relationshipKey( startLabelId, typeId, endLabelId ), count );
        }

        private void verify( Key key, long actualCount )
        {
            MutableLong expected = counts.remove( key );
            if ( expected == null )
            {
                if ( actualCount != 0 )
                {
                    differences.add( new Difference( key, 0, actualCount ) );
                }
            }
            else
            {
                long expectedCount = expected.longValue();
                if ( expectedCount != actualCount )
                {
                    differences.add( new Difference( key, expectedCount, actualCount ) );
                }
            }
        }

        public List<Difference> differences()
        {
            for ( Map.Entry<Key, MutableLong> entry : counts.entrySet() )
            {
                MutableLong value = entry.getValue();
                differences.add( new Difference( entry.getKey(), value.longValue(), 0 ) );
            }
            counts.clear();
            return differences;
        }
    }

    @VisibleForTesting
    public abstract static class Key
    {
        final long[] tokens;

        Key( long... tokens )
        {
            this.tokens = tokens;
        }

        abstract void accept( CountsVisitor visitor, long count );

        @Override
        public boolean equals( Object o )
        {
            return o != null && getClass() == o.getClass() && Arrays.equals( tokens, ((Key) o).tokens );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( tokens );
        }
    }

    @VisibleForTesting
    public static Key nodeKey( long labelId )
    {
        return new Key( labelId )
        {
            @Override
            void accept( CountsVisitor visitor, long count )
            {
                visitor.visitNodeCount( toIntExact( tokens[0] ), count );
            }
        };
    }

    @VisibleForTesting
    public static Key relationshipKey( long startLabelId, long relationshipTypeId, long endLabelId )
    {
        return new Key( startLabelId, relationshipTypeId, endLabelId )
        {
            @Override
            void accept( CountsVisitor visitor, long count )
            {
                visitor.visitRelationshipCount( toIntExact( tokens[0] ), toIntExact( tokens[1] ), toIntExact( tokens[2] ), count );
            }
        };
    }
}
