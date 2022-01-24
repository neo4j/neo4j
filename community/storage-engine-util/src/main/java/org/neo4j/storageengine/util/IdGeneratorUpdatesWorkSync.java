/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.util;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;
import static org.neo4j.internal.id.IdUtils.usedFromCombinedId;

/**
 * Convenience for updating one or more {@link IdGenerator} in a concurrent fashion. Supports applying in batches, e.g. multiple transactions
 * in one go, see {@link #newBatch(CursorContextFactory)}.
 */
public class IdGeneratorUpdatesWorkSync
{
    public static final String ID_GENERATOR_BATCH_APPLIER_TAG = "idGeneratorBatchApplier";

    private final Map<IdGenerator,WorkSync<IdGenerator,IdGeneratorUpdateWork>> workSyncMap = new HashMap<>();

    public void add( IdGenerator idGenerator )
    {
        this.workSyncMap.put( idGenerator, new WorkSync<>( idGenerator ) );
    }

    public Batch newBatch( CursorContextFactory contextFactory )
    {
        return new Batch( contextFactory );
    }

    public class Batch implements IdUpdateListener
    {
        private final Map<IdGenerator,ChangedIds> idUpdatesMap = new HashMap<>();
        private final CursorContextFactory contextFactory;

        protected Batch( CursorContextFactory contextFactory )
        {
            this.contextFactory = contextFactory;
        }

        @Override
        public void markIdAsUsed( IdGenerator idGenerator, long id, int size, CursorContext cursorContext )
        {
            idUpdatesMap.computeIfAbsent( idGenerator, t -> new ChangedIds() ).addUsedId( id, size );
        }

        @Override
        public void markIdAsUnused( IdGenerator idGenerator, long id, int size, CursorContext cursorContext )
        {
            idUpdatesMap.computeIfAbsent( idGenerator, t -> new ChangedIds() ).addUnusedId( id, size );
        }

        public AsyncApply applyAsync()
        {
            // Run through the id changes and apply them, or rather apply them asynchronously.
            // This allows multiple concurrent threads applying batches of transactions to help each other out so that
            // there's a higher chance that changes to different id types can be applied in parallel.
            if ( idUpdatesMap.isEmpty() )
            {
                return AsyncApply.EMPTY;
            }
            applyInternal();
            return this::awaitApply;
        }

        public void apply() throws ExecutionException
        {
            if ( !idUpdatesMap.isEmpty() )
            {
                applyInternal();
                awaitApply();
            }
        }

        private void awaitApply() throws ExecutionException
        {
            // Wait for all id updates to complete
            for ( Map.Entry<IdGenerator,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.awaitApply();
            }
        }

        private void applyInternal()
        {
            for ( Map.Entry<IdGenerator,ChangedIds> idChanges : idUpdatesMap.entrySet() )
            {
                ChangedIds unit = idChanges.getValue();
                unit.applyAsync( workSyncMap.get( idChanges.getKey() ), contextFactory );
            }
        }

        @Override
        public void close() throws Exception
        {
            apply(  );
        }
    }

    private static class ChangedIds
    {
        // The order in which IDs come in, used vs. unused must be kept and therefore all IDs must reside in the same list
        private final MutableLongList ids = LongLists.mutable.empty();
        private AsyncApply asyncApply;

        private void addUsedId( long id, int numberOfIds )
        {
            ids.add( combinedIdAndNumberOfIds( id, numberOfIds, true ) );
        }

        void addUnusedId( long id, int numberOfIds )
        {
            ids.add( combinedIdAndNumberOfIds( id, numberOfIds, false ) );
        }

        void accept( IdGenerator.Marker visitor )
        {
            ids.forEach( combined ->
            {
                long id = idFromCombinedId( combined );
                int slots = numberOfIdsFromCombinedId( combined );
                if ( usedFromCombinedId( combined ) )
                {
                    visitor.markUsed( id, slots );
                }
                else
                {
                    visitor.markDeleted( id, slots );
                }
            } );
        }

        void applyAsync( WorkSync<IdGenerator,IdGeneratorUpdateWork> workSync, CursorContextFactory contextFactory )
        {
            asyncApply = workSync.applyAsync( new IdGeneratorUpdateWork( this, contextFactory ) );
        }

        void awaitApply() throws ExecutionException
        {
            asyncApply.await();
        }
    }

    private static class IdGeneratorUpdateWork implements Work<IdGenerator,IdGeneratorUpdateWork>
    {
        private final List<ChangedIds> changeList = new ArrayList<>();
        private final CursorContextFactory contextFactory;

        IdGeneratorUpdateWork( ChangedIds changes, CursorContextFactory contextFactory )
        {
            this.contextFactory = contextFactory;
            this.changeList.add( changes );
        }

        @Override
        public IdGeneratorUpdateWork combine( IdGeneratorUpdateWork work )
        {
            changeList.addAll( work.changeList );
            return this;
        }

        @Override
        public void apply( IdGenerator idGenerator )
        {
            try (  var cursorContext = contextFactory.create( ID_GENERATOR_BATCH_APPLIER_TAG );
                   IdGenerator.Marker marker = idGenerator.marker( cursorContext ) )
            {
                for ( ChangedIds changes : this.changeList )
                {
                    changes.accept( marker );
                }
            }
        }
    }
}
