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

import org.neo4j.kernel.impl.store.counts.CountsKey;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.RecordState;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static java.util.Objects.requireNonNull;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.counts.CountsKey.indexCountsKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.relationshipKey;
import static org.neo4j.kernel.impl.store.counts.CountsRecordSerializer.DEFAULT_FIRST_VALUE;
import static org.neo4j.kernel.impl.store.counts.CountsRecordSerializer.DEFAULT_SECOND_VALUE;

public class CountsRecordState implements CountsVisitor.Visitable, CountsAccessor, RecordState
{
    private final Map<CountsKey, Register.DoubleLongRegister> counts = new HashMap<>();

    @Override
    public long nodeCount( int labelId )
    {
        return counts( nodeKey( labelId ) ).readSecond();
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        counts( nodeKey( labelId ) ).increment( 0l, delta );
    }

    @Override
    public long relationshipCount( int startLabelId, int typeId, int endLabelId )
    {
        return counts( relationshipKey( startLabelId, typeId, endLabelId ) ).readSecond();
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        counts( indexSampleKey( labelId, propertyKeyId ) ).copyTo( target );
        return target;
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        counts( relationshipKey( startLabelId, typeId, endLabelId ) ).increment( 0l, delta );
    }

    @Override
    public long indexSize( int labelId, int propertyKeyId )
    {
        return counts( indexCountsKey( labelId, propertyKeyId ) ).readSecond();
    }

    @Override
    public long indexUpdates( int labelId, int propertyKeyId )
    {
        return counts( indexCountsKey( labelId, propertyKeyId ) ).readFirst();
    }

    @Override
    public void replaceIndexSize( int labelId, int propertyKeyId, long total )
    {
        counts( indexCountsKey( labelId, propertyKeyId ) ).writeSecond( total );
    }

    @Override
    public void incrementIndexUpdates( int labelId, int propertyKeyId, long delta )
    {
        counts( indexCountsKey( labelId, propertyKeyId ) ).increment( delta, 0l );
    }

    @Override
    public void replaceIndexUpdates( int labelId, int propertyKeyId, long total )
    {
        counts( indexCountsKey( labelId, propertyKeyId ) ).writeFirst( total );
    }

    @Override
    public void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        counts( indexSampleKey( labelId, propertyKeyId ) ).write( unique, size );
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        for ( Map.Entry<CountsKey, Register.DoubleLongRegister> entry : counts.entrySet() )
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
        private final long expectedFirst;
        private final long expectedSecond;
        private final long actualFirst;
        private final long actualSecond;

        public Difference( CountsKey key, long expectedFirst, long expectedSecond, long actualFirst, long actualSecond )
        {
            this.expectedFirst = expectedFirst;
            this.expectedSecond = expectedSecond;
            this.actualFirst = actualFirst;
            this.actualSecond = actualSecond;
            this.key = requireNonNull( key, "key" );
        }

        @Override
        public String toString()
        {
            return String.format( "%s[%s expected=%d:%d, actual=%d:%d]", getClass().getSimpleName(), key, expectedFirst, expectedSecond, actualFirst, actualSecond );
        }

        public CountsKey key()
        {
            return key;
        }

        public long expectedFirstCount()
        {
            return expectedFirst;
        }

        public long expectedSecondCount()
        {
            return expectedSecond;
        }

        public long actualFirstCount()
        {
            return actualFirst;
        }

        public long actualSecondCount()
        {
            return actualSecond;
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
                return actualFirst == that.actualFirst && expectedFirst == that.expectedFirst
                       && actualSecond == that.actualSecond && expectedSecond == that.expectedSecond
                       && key.equals( that.key );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            int result = key.hashCode();
            result = 31 * result + (int) (expectedFirst ^ (expectedFirst >>> 32));
            result = 31 * result + (int) (expectedSecond ^ (expectedSecond >>> 32));
            result = 31 * result + (int) (actualFirst ^ (actualFirst >>> 32));
            result = 31 * result + (int) (actualSecond ^ (actualSecond >>> 32));
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
            for ( long endLabelId : endLabels )
            {
                incrementRelationshipCount( (int) startLabelId, ANY_RELATIONSHIP_TYPE, (int) endLabelId, 1 );
                incrementRelationshipCount( (int) startLabelId, type, (int) endLabelId, 1 );
            }
        }
        for ( long endLabelId : endLabels )
        {
            incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, (int) endLabelId, 1 );
            incrementRelationshipCount( ANY_LABEL, type, (int) endLabelId, 1 );
        }
    }

    private Register.DoubleLongRegister counts( CountsKey key )
    {
        Register.DoubleLongRegister count = counts.get( key );
        if ( count == null )
        {
            count = Registers.newDoubleLongRegister( DEFAULT_FIRST_VALUE, DEFAULT_SECOND_VALUE );
            counts.put( key, count );
        }
        return count;
    }

    private static class CommandCollector extends CountsVisitor.Adapter
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
    }

    private static class Verifier implements CountsVisitor
    {
        private final Map<CountsKey, Register.DoubleLongRegister> counts;
        private final List<Difference> differences = new ArrayList<>();

        Verifier( Map<CountsKey, Register.DoubleLongRegister> counts )
        {
            this.counts = new HashMap<>( counts );
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            verify( nodeKey( labelId ), 0, count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            verify( relationshipKey( startLabelId, typeId, endLabelId ), 0, count );
        }
        @Override
        public void visitIndexCounts( int labelId, int propertyKeyId, long updates, long size )
        {
            verify( indexCountsKey( labelId, propertyKeyId ), updates, size );
        }

        @Override
        public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
        {
            verify( indexSampleKey( labelId, propertyKeyId ), unique, size );
        }

        private void verify( CountsKey key, long actualFirst, long actualSecond )
        {
            Register.DoubleLongRegister expected = counts.remove( key );
            if ( expected == null )
            {
                if ( actualFirst != 0 || actualSecond != 0 )
                {
                    differences.add( new Difference( key, 0, 0, actualFirst, actualSecond ) );
                }
            }
            else {
                long expectedFirst = expected.readFirst();
                long expectedSecond = expected.readSecond();
                if ( expectedFirst != actualFirst || expectedSecond != actualSecond )
                {
                    differences.add( new Difference( key, expectedFirst, expectedSecond, actualFirst, actualSecond ) );
                }
            }
        }

        public List<Difference> differences()
        {
            for ( Map.Entry<CountsKey, Register.DoubleLongRegister> entry : counts.entrySet() )
            {
                Register.DoubleLongRegister value = entry.getValue();
                differences.add( new Difference( entry.getKey(), value.readFirst(), value.readSecond(), 0, 0 ) );
            }
            counts.clear();
            return differences;
        }
    }
}
