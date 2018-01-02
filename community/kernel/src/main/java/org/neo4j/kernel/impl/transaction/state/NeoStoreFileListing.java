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
package org.neo4j.kernel.impl.transaction.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.MetaDataStore;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.SillyUtils.nonNull;
import static org.neo4j.helpers.collection.IteratorUtil.resourceIterator;

public class NeoStoreFileListing
{
    private final File storeDir;
    private final LabelScanStore labelScanStore;
    private final IndexingService indexingService;
    private final LegacyIndexProviderLookup legacyIndexProviders;

    public NeoStoreFileListing( File storeDir, LabelScanStore labelScanStore,
            IndexingService indexingService, LegacyIndexProviderLookup legacyIndexProviders )
    {
        this.storeDir = storeDir;
        this.labelScanStore = labelScanStore;
        this.indexingService = indexingService;
        this.legacyIndexProviders = legacyIndexProviders;
    }

    public ResourceIterator<File> listStoreFiles( boolean includeLogs ) throws IOException
    {
        Collection<File> files = new ArrayList<>();
        gatherNeoStoreFiles( files, includeLogs );
        Resource labelScanStoreSnapshot = gatherLabelScanStoreFiles( files );
        Resource schemaIndexSnapshots = gatherSchemaIndexFiles( files );
        Resource legacyIndexSnapshots = gatherLegacyIndexFiles( files );

        return resourceIterator( files.iterator(),
                new MultiResource( asList( labelScanStoreSnapshot, schemaIndexSnapshots, legacyIndexSnapshots ) ) );
    }

    private Resource gatherLegacyIndexFiles( Collection<File> files ) throws IOException
    {
        final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
        for ( IndexImplementation indexProvider : legacyIndexProviders.all() )
        {
            ResourceIterator<File> snapshot = indexProvider.listStoreFiles();
            snapshots.add( snapshot );
            IteratorUtil.addToCollection( snapshot, files );
        }
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return new MultiResource( snapshots );
    }

    private Resource gatherSchemaIndexFiles(Collection<File> targetFiles) throws IOException
    {
        ResourceIterator<File> snapshot = indexingService.snapshotStoreFiles();
        IteratorUtil.addToCollection(snapshot, targetFiles);
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    private Resource gatherLabelScanStoreFiles( Collection<File> targetFiles ) throws IOException
    {
        ResourceIterator<File> snapshot = labelScanStore.snapshotStoreFiles();
        IteratorUtil.addToCollection(snapshot, targetFiles);
        // Intentionally don't close the snapshot here, return it for closing by the consumer of
        // the targetFiles list.
        return snapshot;
    }

    private void gatherNeoStoreFiles( final Collection<File> targetFiles, boolean includeTransactionLogs )
    {
        File neostoreFile = null;
        for ( File dbFile : nonNull( storeDir.listFiles() ) )
        {
            String name = dbFile.getName();
            if ( dbFile.isFile() )
            {
                if ( name.equals( MetaDataStore.DEFAULT_NAME ) )
                {   // Keep it, to add last
                    neostoreFile = dbFile;
                }
                else if ( neoStoreFile( name ) )
                {
                    targetFiles.add( dbFile );
                }
                else if ( includeTransactionLogs && transactionLogFile( name ) )
                {
                    targetFiles.add( dbFile );
                }
            }
        }
        targetFiles.add( neostoreFile );
    }

    private boolean neoStoreFile( String name )
    {
        if ( name.endsWith( ".id" ) )
        {
            return false;
        }

        if ( name.equals( IndexConfigStore.INDEX_DB_FILE_NAME ) )
        {
            return true;
        }

        return name.startsWith( MetaDataStore.DEFAULT_NAME ) &&
                !name.startsWith( MetaDataStore.DEFAULT_NAME + ".transaction" );
    }

    private boolean transactionLogFile( String name )
    {
        return name.startsWith( MetaDataStore.DEFAULT_NAME + ".transaction" ) && !name.endsWith( ".active" );
    }

    private static final class MultiResource implements Resource
    {
        private final Collection<? extends Resource> snapshots;

        private MultiResource( Collection<? extends Resource> resources )
        {
            this.snapshots = resources;
        }

        @Override
        public void close()
        {
            RuntimeException exception = null;
            for ( Resource snapshot : snapshots )
            {
                try
                {
                    snapshot.close();
                }
                catch ( RuntimeException e )
                {
                    exception = e;
                }
            }
            if ( exception != null )
            {
                throw exception;
            }
        }
    }
}
