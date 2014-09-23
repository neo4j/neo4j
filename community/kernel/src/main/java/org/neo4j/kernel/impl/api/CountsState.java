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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.command.Command;

import static java.util.Objects.requireNonNull;

import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

public class CountsState implements CountsVisitor.Visitable
{
    private final Map<CountsKey, Count> counts = new HashMap<>();

    public void updateCountsForNode( int labelId, int delta )
    {
        count( nodeKey( labelId ) ).update( delta );
    }

    public void updateCountsForRelationship( int startLabelId, int typeId, int endLabelId, int delta )
    {
        count( relationshipKey( startLabelId, typeId, endLabelId ) ).update( delta );
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
            entry.getKey().accept( visitor, entry.getValue().value );
        }
    }

    public void extractCommands( List<Command> target )
    {
        List<Command.CountsCommand> commands = new ArrayList<>( counts.size() );
        accept( new CommandCollector( commands ) );
        target.addAll( commands );
    }

    public List<Difference> verify( CountsVisitor.Visitable visitable )
    {
        Verifier verifier = new Verifier( counts );
        visitable.accept( verifier );
        return verifier.differences();
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

    private Count count( CountsKey key )
    {
        Count count = counts.get( key );
        if ( count == null )
        {
            counts.put( key, count = new Count() );
        }
        return count;
    }

    private static class Count
    {
        long value;

        void update( int delta )
        {
            value += delta;
        }
    }

    private static class CommandCollector implements CountsVisitor
    {
        private final List<Command.CountsCommand> commands;

        CommandCollector( List<Command.CountsCommand> commands )
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
