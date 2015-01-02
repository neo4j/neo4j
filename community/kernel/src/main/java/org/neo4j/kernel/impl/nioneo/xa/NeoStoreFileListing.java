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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;

import static org.neo4j.helpers.SillyUtils.nonNull;

public class NeoStoreFileListing
{
    private final XaContainer xaContainer;
    private final File storeDir;
    private final LabelScanStore labelScanStore;
    private final IndexingService indexingService;
    private Pattern logFilePattern;

    public NeoStoreFileListing(XaContainer xaContainer, File storeDir, LabelScanStore labelScanStore, IndexingService indexingService)
    {
        this.xaContainer = xaContainer;
        this.storeDir = storeDir;
        this.labelScanStore = labelScanStore;
        this.indexingService = indexingService;

        // storing this so we only do Pattern.compile once
        this.logFilePattern = xaContainer.getLogicalLog().getHistoryFileNamePattern();
    }

    public ResourceIterator<File> listStoreFiles( boolean includeLogicalLogs ) throws IOException
    {
        Collection<File> files = new ArrayList<>();
        gatherNeoStoreFiles( includeLogicalLogs, files );
        Resource labelScanStoreSnapshot = gatherLabelScanStoreFiles( files );
        Resource schemaIndexSnapshots = gatherSchemaIndexFiles( files );

        return new StoreSnapshot( files.iterator(), labelScanStoreSnapshot, schemaIndexSnapshots );
    }

    public ResourceIterator<File> listStoreFiles() throws IOException
    {
        Collection<File> files = new ArrayList<>();
        gatherNeoStoreFiles( false, files );
        Resource labelScanStoreSnapshot = gatherLabelScanStoreFiles( files );
        Resource schemaIndexSnapshots = gatherSchemaIndexFiles( files );

        return new StoreSnapshot( files.iterator(), labelScanStoreSnapshot, schemaIndexSnapshots );
    }

    public ResourceIterator<File> listLogicalLogs()
    {
        Collection<File> files = new ArrayList<>();

        for ( File dbFile : nonNull( storeDir.listFiles() ) )
        {
            if ( dbFile.isFile() )
            {
                if ( isLogicalLog( dbFile ) )
                {
                    files.add( dbFile );
                }
            }
        }

        return new StoreSnapshot( files.iterator() );
    }

    private boolean isLogicalLog( File dbFile )
    {
        return logFilePattern.matcher( dbFile.getName() ).matches();
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

    private void gatherNeoStoreFiles( boolean includeLogicalLogs, final Collection<File> targetFiles )
    {
        File neostoreFile = null;
        for ( File dbFile : nonNull( storeDir.listFiles() ) )
        {
            String name = dbFile.getName();
            // To filter for "neostore" is quite future proof, but the "index.db" file
            // maybe should be
            if ( dbFile.isFile() )
            {
                if ( name.equals( NeoStore.DEFAULT_NAME ) )
                {
                    neostoreFile = dbFile;
                }
                else if ( neoStoreFile( name ) )
                {
                    targetFiles.add( dbFile );
                }
                else if ( includeLogicalLogs && isLogicalLog( dbFile ) )
                {
                    targetFiles.add( dbFile );
                }
            }
        }
        targetFiles.add( neostoreFile );
    }

    private boolean neoStoreFile( String name )
    {
        return (name.startsWith( NeoStore.DEFAULT_NAME ) || name.equals( IndexStore.INDEX_DB_FILE_NAME ))
                && !name.endsWith( ".id" );
    }

    private static class StoreSnapshot extends PrefetchingIterator<File> implements ResourceIterator<File>
    {
        private final Iterator<File> files;
        private final Resource[] thingsToCloseWhenDone;

        StoreSnapshot( Iterator<File> files, Resource... thingsToCloseWhenDone )
        {
            this.files = files;
            this.thingsToCloseWhenDone = thingsToCloseWhenDone;
        }

        @Override
        protected File fetchNextOrNull()
        {
            return files.hasNext() ? files.next() : null;
        }

        @Override
        public void close()
        {
            for ( Resource resource : thingsToCloseWhenDone )
            {
                resource.close();
            }
        }
    }

}
