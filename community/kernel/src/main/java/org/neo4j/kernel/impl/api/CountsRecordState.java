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
package org.neo4j.kernel.impl.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.RecordState;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.StorageCommand;

import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.api.StatementConstants.ANY_LABEL;
import static org.neo4j.kernel.api.StatementConstants.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;

public class CountsRecordState implements CountsAccessor, RecordState, CountsAccessor.Updater, CountsAccessor.IndexStatsUpdater
{
    private static final long DEFAULT_FIRST_VALUE = 0;
    private static final long DEFAULT_SECOND_VALUE = 0;
    private final Map<CountsKey, DoubleLongRegister> counts = new HashMap<>();

    @Override
    public DoubleLongRegister nodeCount( int labelId, DoubleLongRegister target )
    {
        counts( nodeKey( labelId ) ).copyTo( target );
        return target;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        counts( nodeKey( labelId ) ).increment( 0L, delta );
    }

    @Override
    public DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
                                                 DoubleLongRegister target )
    {
        counts( relationshipKey( startLabelId, typeId, endLabelId ) ).copyTo( target );
        return target;
    }

    @Override
    public DoubleLongRegister indexSample( long indexId, DoubleLongRegister target )
    {
        counts( indexSampleKey( indexId ) ).copyTo( target );
        return target;
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        if ( delta != 0 )
        {
            counts( relationshipKey( startLabelId, typeId, endLabelId ) ).increment( 0L, delta );
        }
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister target )
    {
        counts( indexStatisticsKey( indexId ) ).copyTo( target );
        return target;
    }

    @Override
    public void replaceIndexUpdateAndSize( long indexId, long updates, long size )
    {
        counts( indexStatisticsKey( indexId ) ).write( updates, size );
    }

    @Override
    public void incrementIndexUpdates( long indexId, long delta )
    {
        counts( indexStatisticsKey( indexId ) ).increment( delta, 0L );
    }

    @Override
    public void replaceIndexSample( long indexId, long unique, long size )
    {
        counts( indexSampleKey( indexId ) ).write( unique, size );
    }

    @Override
    public void close()
    {
        // this is close() of CountsAccessor.Updater - do nothing.
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        for ( Map.Entry<CountsKey, DoubleLongRegister> entry : counts.entrySet() )
        {
            DoubleLongRegister register = entry.getValue();
            entry.getKey().accept( visitor, register.readFirst(), register.readSecond() );
        }
    }

    @Override
    public void extractCommands( Collection<StorageCommand> target )
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
            return String.format( "%s[%s expected=%d:%d, actual=%d:%d]", getClass().getSimpleName(), key, expectedFirst,
                    expectedSecond, actualFirst, actualSecond );
        }

        public CountsKey key()
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
        }
        for ( long endLabelId : endLabels )
        {
            incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, (int) endLabelId, 1 );
            incrementRelationshipCount( ANY_LABEL, type, (int) endLabelId, 1 );
        }
    }

    private DoubleLongRegister counts( CountsKey key )
    {
        return counts.computeIfAbsent( key,
                k -> Registers.newDoubleLongRegister( DEFAULT_FIRST_VALUE, DEFAULT_SECOND_VALUE ) );
    }

    private static class CommandCollector extends CountsVisitor.Adapter
    {
        private final Collection<StorageCommand> commands;

        CommandCollector( Collection<StorageCommand> commands )
        {
            this.commands = commands;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new Command.NodeCountsCommand( labelId, count ) );
            }
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new Command.RelationshipCountsCommand( startLabelId, typeId, endLabelId, count ) );
            }
        }
    }

    private static class Verifier implements CountsVisitor
    {
        private final Map<CountsKey, DoubleLongRegister> counts;
        private final List<Difference> differences = new ArrayList<>();

        Verifier( Map<CountsKey, DoubleLongRegister> counts )
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
        public void visitIndexStatistics( long indexId, long updates, long size )
        {
            verify( indexStatisticsKey( indexId ), updates, size );
        }

        @Override
        public void visitIndexSample( long indexId, long unique, long size )
        {
            verify( indexSampleKey( indexId ), unique, size );
        }

        private void verify( CountsKey key, long actualFirst, long actualSecond )
        {
            DoubleLongRegister expected = counts.remove( key );
            if ( expected == null )
            {
                if ( actualFirst != 0 || actualSecond != 0 )
                {
                    differences.add( new Difference( key, 0, 0, actualFirst, actualSecond ) );
                }
            }
            else
            {
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
            for ( Map.Entry<CountsKey, DoubleLongRegister> entry : counts.entrySet() )
            {
                DoubleLongRegister value = entry.getValue();
                differences.add( new Difference( entry.getKey(), value.readFirst(), value.readSecond(), 0, 0 ) );
            }
            counts.clear();
            return differences;
        }
    }
}
