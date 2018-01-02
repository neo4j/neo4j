/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.LazyIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;

/**
 * Performs validation of index updates for transactions based on
 * {@link org.neo4j.kernel.impl.transaction.command.Command}s in transaction state.
 * It is done by inferring {@link org.neo4j.kernel.api.index.NodePropertyUpdate}s from commands and asking
 * {@link org.neo4j.kernel.impl.api.index.IndexingService} to check those via
 * {@link org.neo4j.kernel.impl.api.index.IndexingService#validate(Iterable,IndexUpdateMode)}.
 */
public class OnlineIndexUpdatesValidator implements IndexUpdatesValidator
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final PropertyLoader propertyLoader;
    private final IndexingService indexing;
    private final KernelHealth kernelHealth;
    private final IndexUpdateMode updateMode;

    public OnlineIndexUpdatesValidator( NeoStores neoStore, KernelHealth kernelHealth, PropertyLoader propertyLoader,
            IndexingService indexing, IndexUpdateMode updateMode )
    {
        this.kernelHealth = kernelHealth;
        this.updateMode = updateMode;
        this.nodeStore = neoStore.getNodeStore();
        this.propertyStore = neoStore.getPropertyStore();
        this.propertyLoader = propertyLoader;
        this.indexing = indexing;
    }

    @Override
    public ValidatedIndexUpdates validate( TransactionRepresentation transaction ) throws IOException
    {
        NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
        try
        {
            transaction.accept( extractor );
        }
        catch ( IOException cause )
        {
            kernelHealth.panic( cause );
            throw cause;
        }

        if ( !extractor.containsAnyNodeOrPropertyUpdate() )
        {
            return ValidatedIndexUpdates.NONE;
        }

        Iterable<NodePropertyUpdate> updates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                extractor.propertyCommandsByNodeIds, extractor.nodeCommandsById );

        return indexing.validate( updates, updateMode );
    }
}
