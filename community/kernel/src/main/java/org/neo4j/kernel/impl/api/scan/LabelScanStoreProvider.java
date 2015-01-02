/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.DependencyResolver.SelectionStrategy;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.util.AbstractPrimitiveLongIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.Integer.MAX_VALUE;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.kernel.extension.KernelExtensionUtil.servicesClassPathEntryInformation;

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
    /**
     * SelectionStrategy for {@link KernelExtensions kernel extensions loading} where the one with highest
     * {@link #priority} will be selected. If there are no such stores  then an {@link IllegalStateException} will be
     * thrown.
     */
    public static SelectionStrategy HIGHEST_PRIORITIZED =
            new SelectionStrategy()
    {
        @Override
        public <T> T select( Class<T> type, Iterable<T> candidates )
                throws IllegalArgumentException
        {
            List<Comparable> all = (List<Comparable>) addToCollection( candidates, new ArrayList<T>() );
            if ( all.isEmpty() )
            {
                throw new IllegalArgumentException( "No label scan store provider " +
                        LabelScanStoreProvider.class.getName() + " found. " + servicesClassPathEntryInformation() );
            }
            Collections.sort( all );
            return (T) all.get( all.size()-1 );
        }
    };

    private final LabelScanStore labelScanStore;

    private final int priority;

    public LabelScanStoreProvider( LabelScanStore labelScanStore, int priority )
    {
        this.labelScanStore = labelScanStore;
        this.priority = priority;
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

    public interface FullStoreChangeStream extends Iterable<NodeLabelUpdate>
    {
        PrimitiveLongIterator labelIds();

        long highestNodeId();
    }

    public static FullStoreChangeStream fullStoreLabelUpdateStream( final NeoStoreProvider neoStoreProvider )
    {
        return new FullStoreChangeStream()
        {
            @Override
            public Iterator<NodeLabelUpdate> iterator()
            {
                return new PrefetchingIterator<NodeLabelUpdate>()
                {
                    private final long[] NO_LABELS = new long[0];
                    private final NodeStore nodeStore = neoStoreProvider.evaluate().getNodeStore();
                    private final long highId = nodeStore.getHighestPossibleIdInUse();
                    private long current;

                    @Override
                    protected NodeLabelUpdate fetchNextOrNull()
                    {
                        while ( current <= highId )
                        {
                            NodeRecord node = nodeStore.forceGetRecord( current++ );
                            if ( node.inUse() )
                            {
                                long[] labels = NodeLabelsField.parseLabelsField( node ).get( nodeStore );
                                if ( labels.length > 0 )
                                {
                                    return NodeLabelUpdate.labelChanges( node.getId(), NO_LABELS, labels );
                                }
                            }
                        }
                        return null;
                    }
                };
            }

            @Override
            public PrimitiveLongIterator labelIds()
            {
                final Token[] labels = neoStoreProvider.evaluate().getLabelTokenStore().getTokens( MAX_VALUE );
                return new AbstractPrimitiveLongIterator()
                {
                    int index;
                    {
                        computeNext();
                    }

                    @Override
                    protected void computeNext()
                    {
                        if ( index <= labels.length )
                        {
                            next( labels[index++].id() );
                        }
                        else
                        {
                            endReached();
                        }
                    }
                };
            }

            @Override
            public long highestNodeId()
            {
                return neoStoreProvider.evaluate().getNodeStore().getHighestPossibleIdInUse();
            }
        };
    }
}
