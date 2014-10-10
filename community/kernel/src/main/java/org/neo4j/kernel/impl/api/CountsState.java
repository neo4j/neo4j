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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.RecordState;
import org.neo4j.register.Register;

import static java.util.Objects.requireNonNull;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.api.CountsKey.indexKey;
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

public class CountsState implements CountsVisitor.Visitable, CountsAcceptor, RecordState
{
    private final Map<CountsKey, Count> counts = new HashMap<>();

    @Override
    public void updateCountsForNode( int labelId, long delta )
    {
        count( nodeKey( labelId ) ).update( delta );
    }

    @Override
    public void updateCountsForRelationship( int startLabelId, int typeId, int endLabelId, long delta )
    {
        count( relationshipKey( startLabelId, typeId, endLabelId ) ).update( delta );
    }

    @Override
    public void updateCountsForIndex( int indexId, long delta )
    {
        count( indexKey( indexId ) ).update( delta );
    }

    public void increment( int labelId )
    {
        updateCountsForNode( labelId, 1 );
    }

    public void decrement( int labelId )
    {
        updateCountsForNode( labelId, -1 );
    }

    public void increment( int startLabelId, int typeId, int endLabelId )
    {
        updateCountsForRelationship( startLabelId, typeId, endLabelId, 1 );
    }

    public void decrement( int startLabelId, int typeId, int endLabelId )
    {
        updateCountsForRelationship( startLabelId, typeId, endLabelId, -1 );
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        for ( Map.Entry<CountsKey, Count> entry : counts.entrySet() )
        {
            entry.getKey().accept( visitor, entry.getValue() );
        }
    }

    @Override
    public void extractCommands( Collection<Command> target )
    {
        accept( new CommandCollector( target ) );
    }

    public List<Difference> verify( CountsVisitor.Visitable visitable )
    {
        Verifier verifier = new Verifier( counts );
        visitable.accept( verifier );
        return verifier.differences();
    }

    @Override
    public boolean hasChanges()
    {
        return !counts.isEmpty();
    }

    /**
     * Set this counter up to a pristine state, as if it had just been initialized.
     */
    public void initialize()
    {
        if ( !counts.isEmpty() )
        {
            counts.clear();
        }
    }

    public static final class Difference
    {
        private final CountsKey key;
        private final long expected;
        private final long actual;

        public Difference( CountsKey key, long expected, long actual )
        {
            this.key = requireNonNull( key, "key" );
            this.expected = expected;
            this.actual = actual;
        }

        @Override
        public String toString()
        {
            return String.format( "%s[%s expected=%d, actual=%d]", getClass().getSimpleName(), key, expected, actual );
        }

        public CountsKey key()
        {
            return key;
        }

        public long expectedCount()
        {
            return expected;
        }

        public long actualCount()
        {
            return actual;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( (obj instanceof Difference) )
            {
                Difference that = (Difference) obj;
                return actual == that.actual && expected == that.expected && key.equals( that.key );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            int result = key.hashCode();
            result = 31 * result + (int) (expected ^ (expected >>> 32));
            result = 31 * result + (int) (actual ^ (actual >>> 32));
            return result;
        }
    }

    public void addNode( long[] labels )
    {
        increment( ANY_LABEL );
        for ( long label : labels )
        {
            increment( (int) label );
        }
    }

    public void addRelationship( long[] startLabels, int type, long[] endLabels )
    {
        increment( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL );
        increment( ANY_LABEL, type, ANY_LABEL );
        for ( long startLabelId : startLabels )
        {
            increment( (int) startLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL );
            increment( (int) startLabelId, type, ANY_LABEL );
            for ( long endLabelId : endLabels )
            {
                increment( (int) startLabelId, ANY_RELATIONSHIP_TYPE, (int) endLabelId );
                increment( (int) startLabelId, type, (int) endLabelId );
            }
        }
        for ( long endLabelId : endLabels )
        {
            increment( ANY_LABEL, ANY_RELATIONSHIP_TYPE, (int) endLabelId );
            increment( ANY_LABEL, type, (int) endLabelId );
        }
    }

    private Count count( CountsKey key )
    {
        Count count = counts.get( key );
        if ( count == null )
        {
            counts.put( key, count = new Count() );
        }
        return count;
    }

    private static class Count implements Register.LongRegister
    {
        private long value;

        void update( long delta )
        {
            value += delta;
        }

        @Override
        public String toString()
        {
            return Long.toString( value );
        }

        @Override
        public long read()
        {
            return value;
        }

        @Override
        public void write( long value )
        {
            this.value = value;
        }
    }

    private static class CommandCollector implements CountsVisitor
    {
        private final Collection<Command> commands;

        CommandCollector( Collection<Command> commands )
        {
            this.commands = commands;
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new Command.CountsCommand().init( startLabelId, typeId, endLabelId, count ) );
            }
        }

        @Override
        public void visitIndexCount( int indexId, long count )
        {
            // not updated through commands
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            // not updated through commands
        }
    }

    private static class Verifier implements CountsVisitor
    {
        private final Map<CountsKey, Count> counts;
        private final List<Difference> differences = new ArrayList<>();

        Verifier( Map<CountsKey, Count> counts )
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

        @Override
        public void visitIndexCount( int indexId, long count )
        {
            verify( indexKey( indexId ), count );
        }

        private void verify( CountsKey key, long actual )
        {
            Count expected = counts.remove( key );
            if ( expected == null )
            {
                if ( actual != 0 )
                {
                    differences.add( new Difference( key, 0, actual ) );
                }
            }
            else if ( expected.value != actual )
            {
                differences.add( new Difference( key, expected.value, actual ) );
            }
        }

        public List<Difference> differences()
        {
            for ( Map.Entry<CountsKey, Count> entry : counts.entrySet() )
            {
                differences.add( new Difference( entry.getKey(), entry.getValue().value, 0 ) );
            }
            counts.clear();
            return differences;
        }
    }
}
