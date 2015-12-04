/*
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.internal.DatabaseHealth;
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
    private final DatabaseHealth databaseHealth;
    private final IndexUpdateMode updateMode;

    public OnlineIndexUpdatesValidator( NeoStores neoStore, DatabaseHealth databaseHealth, PropertyLoader propertyLoader,
                                        IndexingService indexing, IndexUpdateMode updateMode )
    {
        this.databaseHealth = databaseHealth;
        this.updateMode = updateMode;
        this.nodeStore = neoStore.getNodeStore();
        this.propertyStore = neoStore.getPropertyStore();
        this.propertyLoader = propertyLoader;
        this.indexing = indexing;
    }

    @Override
    public ValidatedIndexUpdates validate( TransactionRepresentation transaction ) throws IOException
    {
        // NodePropertyCommandsExtractor doesn't actually do anything in close atm, but wrap in try-with-resources
        // in case it will in the future
        try ( NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor() )
        {
            try ( TransactionApplier txApplier = extractor.startTx( new TransactionToApply( transaction ) ))
            {
                transaction.accept( txApplier );
            }
            catch ( IOException cause )
            {
                databaseHealth.panic( cause );
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
        catch ( Exception cause )
        {
            databaseHealth.panic( cause );
            throw new IOException( cause );
        }
    }
}
