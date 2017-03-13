/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

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
public class LabelScanStoreProvider extends LifecycleAdapter
{
    private static final String KEY = "lucene";

    private final String name;
    private final LabelScanStore labelScanStore;

    public LabelScanStoreProvider( String name, LabelScanStore labelScanStore )
    {
        this.name = name;
        this.labelScanStore = labelScanStore;
    }

    public String getName()
    {
        return name;
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
    public String toString()
    {
        return getClass().getSimpleName() + "[" + labelScanStore + "]";
    }

    public static long rebuild( LabelScanStore store, FullStoreChangeStream fullStoreStream ) throws IOException
    {
        try ( LabelScanWriter writer = store.newWriter() )
        {
            return fullStoreStream.applyTo( writer );
        }
    }

    public boolean isReadOnly()
    {
        return labelScanStore.isReadOnly();
    }

    public void drop() throws IOException
    {
        labelScanStore.drop();
    }

    public boolean hasStore() throws IOException
    {
        return labelScanStore.hasStore();
    }
}
