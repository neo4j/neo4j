/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.factory.Sets;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;

public class GBPTreeBootstrapper implements Closeable
{
    private static final int MAX_PAGE_SIZE = (int) ByteUnit.mebiBytes( 4 );
    private final FileSystemAbstraction fs;
    private final JobScheduler jobScheduler;
    private final LayoutBootstrapper layoutBootstrapper;
    private final boolean readOnly;
    private final PageCacheTracer pageCacheTracer;
    private PageCache pageCache;

    public GBPTreeBootstrapper( FileSystemAbstraction fs, JobScheduler jobScheduler, LayoutBootstrapper layoutBootstrapper, boolean readOnly,
            PageCacheTracer pageCacheTracer )
    {
        this.fs = fs;
        this.jobScheduler = jobScheduler;
        this.layoutBootstrapper = layoutBootstrapper;
        this.readOnly = readOnly;
        this.pageCacheTracer = pageCacheTracer;
    }

    public Bootstrap bootstrapTree( Path file )
    {
        try
        {
            instantiatePageCache( fs, jobScheduler, PageCache.PAGE_SIZE );
            // Get meta information about the tree
            MetaVisitor<?,?> metaVisitor = visitMeta( file );
            Meta meta = metaVisitor.meta;
            if ( !isReasonablePageSize( meta.getPageSize() ) )
            {
                throw new MetadataMismatchException( "Unexpected page size " + meta.getPageSize() );
            }
            if ( meta.getPageSize() != pageCache.pageSize() )
            {
                // GBPTree was created with a different page size, re-instantiate page cache and re-read meta.
                instantiatePageCache( fs, jobScheduler, meta.getPageSize() );
                metaVisitor = visitMeta( file );
                meta = metaVisitor.meta;
            }
            StateVisitor<?,?> stateVisitor = visitState( file );
            Pair<TreeState,TreeState> statePair = stateVisitor.statePair;
            TreeState state = TreeStatePair.selectNewestValidState( statePair );

            // Create layout and treeNode from meta
            Layout<?,?> layout = layoutBootstrapper.create( file, pageCache, meta );
            GBPTree<?,?> tree = new GBPTree<>( pageCache, file, layout, NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER, ignore(), readOnly,
                    pageCacheTracer, Sets.immutable.empty(), file.getFileName().toString() );
            return new SuccessfulBootstrap( tree, layout, state, meta );
        }
        catch ( Exception e )
        {
            return new FailedBootstrap( e );
        }
    }

    @Override
    public void close() throws IOException
    {
        closePageCache();
    }

    private MetaVisitor<?,?> visitMeta( Path file ) throws IOException
    {
        MetaVisitor<?,?> metaVisitor = new MetaVisitor();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "TreeBootstrap" ) )
        {
            GBPTreeStructure.visitMeta( pageCache, file, metaVisitor, cursorTracer );
        }
        return metaVisitor;
    }

    private StateVisitor<?,?> visitState( Path file ) throws IOException
    {
        StateVisitor<?,?> stateVisitor = new StateVisitor();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "TreeBootstrap" ) )
        {
            GBPTreeStructure.visitState( pageCache, file, stateVisitor, cursorTracer );
        }
        return stateVisitor;
    }

    private void instantiatePageCache( FileSystemAbstraction fs, JobScheduler jobScheduler, int pageSize )
    {
        if ( pageCache != null && pageCache.pageSize() == pageSize )
        {
            return;
        }
        closePageCache();
        var swapper = new SingleFilePageSwapperFactory( fs );
        long expectedMemory = Math.max( MuninnPageCache.memoryRequiredForPages( 100 ), 3 * pageSize );
        pageCache = new MuninnPageCache( swapper, jobScheduler, config( createAllocator( expectedMemory, EmptyMemoryTracker.INSTANCE ) ).pageSize( pageSize ) );
    }

    private void closePageCache()
    {
        if ( pageCache != null )
        {
            pageCache.close();
            pageCache = null;
        }
    }

    private static boolean isReasonablePageSize( int number )
    {
        return isPowerOfTwo( number ) && isReasonableSize( number );
    }

    private static boolean isReasonableSize( int pageSize )
    {
        return pageSize <= MAX_PAGE_SIZE;
    }

    public interface Bootstrap
    {
        boolean isTree();
        GBPTree<?,?> getTree();
        Layout<?,?> getLayout();
        TreeState getState();
        Meta getMeta();
    }

    private static class FailedBootstrap implements Bootstrap
    {
        private final Throwable cause;

        FailedBootstrap( Throwable cause )
        {
            this.cause = cause;
        }

        @Override
        public boolean isTree()
        {
            return false;
        }

        @Override
        public GBPTree<?,?> getTree()
        {
            throw new IllegalStateException( "Bootstrap failed", cause );
        }

        @Override
        public Layout<?,?> getLayout()
        {
            throw new IllegalStateException( "Bootstrap failed", cause );
        }

        @Override
        public TreeState getState()
        {
            throw new IllegalStateException( "Bootstrap failed", cause );
        }

        @Override
        public Meta getMeta()
        {
            throw new IllegalStateException( "Bootstrap failed", cause );
        }
    }

    private static class SuccessfulBootstrap implements Bootstrap
    {
        private final GBPTree<?,?> tree;
        private final Layout<?,?> layout;
        private final TreeState state;
        private final Meta meta;

        SuccessfulBootstrap( GBPTree<?,?> tree, Layout<?,?> layout, TreeState state, Meta meta )
        {
            this.tree = tree;
            this.layout = layout;
            this.state = state;
            this.meta = meta;
        }

        @Override
        public boolean isTree()
        {
            return true;
        }

        @Override
        public GBPTree<?,?> getTree()
        {
            return tree;
        }

        @Override
        public Layout<?,?> getLayout()
        {
            return layout;
        }

        @Override
        public TreeState getState()
        {
            return state;
        }

        @Override
        public Meta getMeta()
        {
            return meta;
        }
    }

    private static class MetaVisitor<KEY,VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
    {
        private Meta meta;
        @Override
        public void meta( Meta meta )
        {
            this.meta = meta;
        }
    }

    private static class StateVisitor<KEY,VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
    {
        private Pair<TreeState,TreeState> statePair;

        @Override
        public void treeState( Pair<TreeState,TreeState> statePair )
        {
            this.statePair = statePair;
        }
    }
}
