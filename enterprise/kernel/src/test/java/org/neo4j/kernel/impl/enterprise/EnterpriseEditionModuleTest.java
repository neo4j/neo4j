/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise;

import org.junit.Test;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EnterpriseEditionModuleTest
{
    @Test
    public void fileWatcherFileNameFilter()
    {
        Predicate<String> filter = EnterpriseEditionModule.enterpriseNonClusterFileWatcherFileNameFilter();
        assertFalse( filter.test( MetaDataStore.DEFAULT_NAME ) );
        assertFalse( filter.test( StoreFile.NODE_STORE.fileName( StoreFileType.STORE ) ) );
        assertTrue( filter.test( TransactionLogFiles.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( IndexConfigStore.INDEX_DB_FILE_NAME + ".any" ) );
        assertTrue( filter.test( MetaDataStore.DEFAULT_NAME + PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
