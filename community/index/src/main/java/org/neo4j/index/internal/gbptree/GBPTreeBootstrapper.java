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
    private final LayoutBootstrapper layoutBootstrapper;
    private final boolean readOnly;

    public GBPTreeBootstrapper( PageCache pageCache, LayoutBootstrapper layoutBootstrapper, boolean readOnly )
    {
        this.pageCache = pageCache;
        this.layoutBootstrapper = layoutBootstrapper;
        this.readOnly = readOnly;
    }

    public Bootstrap bootstrapTree( File file, String targetLayout )
    {
        try
        {
            // Get meta information about the tree
            MetaVisitor<?,?> metaVisitor = new MetaVisitor();
            GBPTreeStructure.visitHeader( pageCache, file, metaVisitor );
            Meta meta = metaVisitor.meta;
            Pair<TreeState,TreeState> statePair = metaVisitor.statePair;
            TreeState state = TreeStatePair.selectNewestValidState( statePair );

            // Create layout and treeNode from meta
            Layout<?,?> layout = layoutBootstrapper.create( file, pageCache, meta, targetLayout );
            TreeNodeSelector.Factory factory = TreeNodeSelector.selectByFormat( meta.getFormatIdentifier(), meta.getFormatVersion() );
            TreeNode<?,?> treeNode = factory.create( meta.getPageSize(), layout );
            GBPTree<?,?> tree =
                    new GBPTree<>( pageCache, file, layout, meta.getPageSize(), NO_MONITOR, NO_HEADER_READER, NO_HEADER_WRITER, ignore(), readOnly );
            return new SuccessfulBootstrap( tree, layout, treeNode, state, meta );
        }
        catch ( Exception e )
        {
            return new FailedBootstrap();
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

    public interface Bootstrap
    {
        boolean isTree();
        GBPTree<?,?> getTree();
        Layout<?,?> getLayout();
        TreeNode<?,?> getTreeNode();
        TreeState getState();
        Meta getMeta();
    }

    private static class FailedBootstrap implements Bootstrap
    {
        @Override
        public boolean isTree()
        {
            return false;
        }

        @Override
        public GBPTree<?,?> getTree()
        {
            throw new IllegalStateException( "Bootstrap failed" );
        }

        @Override
        public Layout<?,?> getLayout()
        {
            throw new IllegalStateException( "Bootstrap failed" );
        }

        @Override
        public TreeNode<?,?> getTreeNode()
        {
            throw new IllegalStateException( "Bootstrap failed" );
        }

        @Override
        public TreeState getState()
        {
            throw new IllegalStateException( "Bootstrap failed" );
        }

        @Override
        public Meta getMeta()
        {
            throw new IllegalStateException( "Bootstrap failed" );
        }
    }

    private static class SuccessfulBootstrap implements Bootstrap
    {
        private final GBPTree<?,?> tree;
        private final Layout<?,?> layout;
        private final TreeNode<?,?> treeNode;
        private final TreeState state;
        private final Meta meta;

        SuccessfulBootstrap( GBPTree<?,?> tree, Layout<?,?> layout, TreeNode<?,?> treeNode, TreeState state, Meta meta )
        {
            this.tree = tree;
            this.layout = layout;
            this.treeNode = treeNode;
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
        public TreeNode<?,?> getTreeNode()
        {
            return treeNode;
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
