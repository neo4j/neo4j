/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.scan;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * Used by a {@link KernelExtensions} to provide access a {@link LabelScanStore} and prioritize against other.
 *
 * (Kernel extension loading mechanism)-[:FINDS]->(KernelExtensionFactory)-[:THAT_PRODUCES]->(LabelScanStoreProvider)-
 *     -[:THAT_PROVIDES_ACCESS_TO_AND_PRIORITIZES]->(LabelScanStore)
 *
 * Explicitly don't forward {@link Lifecycle} calls to the {@link LabelScanStore} itself since this provider
 * participates in KernelExtensions' life cycle, but there are assumptions in and around LabelScanStores that
 * require its life cycle to be managed by a data source, which has its own little sub-life cycle. I.e. it will
 * get the {@link LabelScanStore} from this provider, stick it in an e.g. {@link LifeSupport} of its own.
 * {@link LabelScanStoreProvider} implements {@link Lifecycle} to adhere to {@link KernelExtensionFactory} contract.
 */
public class LabelScanStoreProvider extends LifecycleAdapter implements Comparable<LabelScanStoreProvider>
{
    private static final String KEY = "lucene";

    private final LabelScanStore labelScanStore;
    private final int priority;

    public LabelScanStoreProvider( LabelScanStore labelScanStore, int priority )
    {
        this.labelScanStore = labelScanStore;
        this.priority = priority;
    }

    public static File getStoreDirectory( File storeRootDir )
    {
        return new File( new File( new File( storeRootDir, "schema" ), "label" ), KEY );
    }

    public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }

    @Override
    public int compareTo( LabelScanStoreProvider o )
    {
        return priority - o.priority;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + labelScanStore + ", prio:" + priority + "]";
    }

    public interface FullStoreChangeStream
    {
        long applyTo( LabelScanWriter writer ) throws IOException;
    }

    public static FullStoreChangeStream fullStoreLabelUpdateStream( Supplier<IndexStoreView> lazyIndexStoreView )
    {
        // IndexStoreView provided as supplier because we only actually have that dependency available
        // when it's time to rebuilt, not when we construct this object
        return new FullLabelStream( lazyIndexStoreView );
    }

    private static class FullLabelStream implements FullStoreChangeStream, Visitor<NodeLabelUpdate,IOException>
    {
        private final Supplier<IndexStoreView> lazyIndexStoreView;
        private LabelScanWriter writer;
        private long count;

        public FullLabelStream( Supplier<IndexStoreView> lazyIndexStoreView )
        {
            this.lazyIndexStoreView = lazyIndexStoreView;
        }

        @Override
        public long applyTo( LabelScanWriter writer ) throws IOException
        {
            // Keep the write for using it in visit
            this.writer = writer;
            IndexStoreView view = lazyIndexStoreView.get();
            StoreScan<IOException> scan = view.visitNodes( ArrayUtils.EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null, this );
            scan.run();
            return count;
        }

        @Override
        public boolean visit( NodeLabelUpdate update ) throws IOException
        {
            writer.write( update );
            count++;
            return false;
        }
    }
}
