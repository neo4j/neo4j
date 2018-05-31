/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.register.Register.DoubleLongRegister;

public interface CountsAccessor extends CountsVisitor.Visitable
{
    /**
     * @param target a register to store the read values in
     * @return the input register for convenience
     */
    DoubleLongRegister nodeCount( int labelId, DoubleLongRegister target );

    /**
     * @param target a register to store the read values in
     * @return the input register for convenience
     */
    DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId, DoubleLongRegister target );

    /**
     * @param target a register to store the read values in
     * @return the input register for convenience
     */
    DoubleLongRegister indexUpdatesAndSize( long indexId, DoubleLongRegister target );

    /**
     * @param target a register to store the read values in
     * @return the input register for convenience
     */
    DoubleLongRegister indexSample( long indexId, DoubleLongRegister target );

    interface Updater extends AutoCloseable
    {
        void incrementNodeCount( long labelId, long delta );

        void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta );

        @Override
        void close();
    }

    interface IndexStatsUpdater extends AutoCloseable
    {
        void replaceIndexUpdateAndSize( long indexId, long updates, long size );

        void replaceIndexSample( long indexId, long unique, long size );

        void incrementIndexUpdates( long indexId, long delta );

        @Override
        void close();
    }

    final class Initializer implements CountsVisitor
    {
        private final Updater updater;
        private final IndexStatsUpdater stats;

        public Initializer( Updater updater, IndexStatsUpdater stats )
        {
            this.updater = updater;
            this.stats = stats;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            updater.incrementNodeCount( labelId, count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            updater.incrementRelationshipCount( startLabelId, typeId, endLabelId, count );
        }

        @Override
        public void visitIndexStatistics( long indexId, long updates, long size )
        {
            stats.replaceIndexUpdateAndSize( indexId, updates, size );
        }

        @Override
        public void visitIndexSample( long indexId, long unique, long size )
        {
            stats.replaceIndexSample( indexId, unique, size );
        }
    }
}
