/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

/**
 * Migrates a neo4j database from one version to the next. Instantiated with a {@link LegacyStore}
 * representing the old version and a {@link NeoStore} representing the new version.
 * 
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 */
public class StoreMigrator
{
    private final MigrationProgressMonitor progressMonitor;

    public StoreMigrator( MigrationProgressMonitor progressMonitor )
    {
        this.progressMonitor = progressMonitor;
    }

    public void migrate( LegacyStore legacyStore, NeoStore neoStore ) throws IOException
    {
        progressMonitor.started();
        new Migration( legacyStore, neoStore ).migrate();
        progressMonitor.finished();
    }
    
    protected class Migration
    {
        private final LegacyStore legacyStore;
        private final NeoStore neoStore;
        private final long totalEntities;
        private int percentComplete;

        public Migration( LegacyStore legacyStore, NeoStore neoStore )
        {
            this.legacyStore = legacyStore;
            this.neoStore = neoStore;
            totalEntities = legacyStore.getNodeStoreReader().getMaxId();
        }

        private void migrate() throws IOException
        {
            // Migrate
            migrateNodes( neoStore.getNodeStore() );
            
            // Close
            neoStore.close();
            legacyStore.close();
            
            // Just copy unchanged stores that doesn't need migration
            legacyStore.copyNeoStore( neoStore );
            legacyStore.copyRelationshipStore( neoStore );
            legacyStore.copyRelationshipTypeStore( neoStore );
            legacyStore.copyRelationshipTypeNameStore( neoStore );
            legacyStore.copyPropertyStore( neoStore );
            legacyStore.copyPropertyIndexStore( neoStore );
            legacyStore.copyPropertyIndexNameStore( neoStore );
            legacyStore.copyDynamicStringPropertyStore( neoStore );
            legacyStore.copyDynamicArrayPropertyStore( neoStore );
        }
        
        private void migrateNodes( NodeStore nodeStore ) throws IOException
        {
            Iterable<NodeRecord> records = legacyStore.getNodeStoreReader().readNodeStore();
            for ( NodeRecord nodeRecord : records )
            {
                reportProgress( nodeRecord.getId() );
                nodeStore.setHighId( nodeRecord.getId() + 1 );
                if ( nodeRecord.inUse() )
                {
                    nodeStore.updateRecord( nodeRecord );
                }
                else
                {
                    nodeStore.freeId( nodeRecord.getId() );
                }
            }
            legacyStore.getNodeStoreReader().close();
        }
        
        private void reportProgress( long id )
        {
            int newPercent = (int) (id * 100 / totalEntities);
            if ( newPercent > percentComplete ) {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }
    }
}
