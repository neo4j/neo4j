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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.graphdb.config.Configuration.EMPTY;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

public class GBPTreeBootstrapper
{
    private final PageCache pageCache;
    private final MetaToLayoutFactory layoutFactory;
    private final boolean readOnly;

    public GBPTreeBootstrapper( PageCache pageCache, MetaToLayoutFactory layoutFactory, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.layoutFactory = layoutFactory;
        this.readOnly = readOnly;
    }

    public Bootstrap bootstrapTree( File file, String targetLayout ) throws IOException
    {
        // Get meta information about the tree
        MetaVisitor<?,?> metaVisitor = new MetaVisitor();
        GBPTreeStructure.visitHeader( pageCache, file, metaVisitor );
        Meta meta = metaVisitor.meta;
        Pair<TreeState,TreeState> statePair = metaVisitor.statePair;
        TreeState state = TreeStatePair.selectNewestValidState( statePair );

        // Create layout and treeNode from meta
        Layout<?,?> layout = layoutFactory.create( meta, targetLayout );
        TreeNodeSelector.Factory factory = TreeNodeSelector.selectByFormat( meta.getFormatIdentifier(), meta.getFormatVersion() );
        TreeNode<?,?> treeNode = factory.create( meta.getPageSize(), layout );
        GBPTree<?,?> tree = new GBPTree<>( pageCache, file, layout, meta.getPageSize(), NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER, ignore(), readOnly );
        return new Bootstrap( tree, layout, treeNode, state, meta );
    }

    public class Bootstrap
    {
        public final GBPTree<?,?> tree;
        public final Layout<?,?> layout;
        public final TreeNode<?,?> treeNode;
        public final TreeState state;
        public final Meta meta;

        Bootstrap( GBPTree<?,?> tree, Layout<?,?> layout, TreeNode<?,?> treeNode, TreeState state, Meta meta )
        {
            this.tree = tree;
            this.layout = layout;
            this.treeNode = treeNode;
            this.state = state;
            this.meta = meta;
        }
    }

    static PageCache pageCache( JobScheduler jobScheduler )
    {
        SingleFilePageSwapperFactory swapper = new SingleFilePageSwapperFactory();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        swapper.open( fs, EMPTY );
        PageCursorTracerSupplier cursorTracerSupplier = PageCursorTracerSupplier.NULL;
        return new MuninnPageCache( swapper, 100, NULL, cursorTracerSupplier, EmptyVersionContextSupplier.EMPTY, jobScheduler );
    }

    private static class MetaVisitor<KEY,VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
    {
        private Meta meta;
        private Pair<TreeState,TreeState> statePair;

        @Override
        public void meta( Meta meta )
        {
            this.meta = meta;
        }

        @Override
        public void treeState( Pair<TreeState,TreeState> statePair )
        {
            this.statePair = statePair;
        }
    }
}
