/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import java.util.function.Predicate;

import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EnterpriseReadReplicaEditionModuleTest
{
    @Test
    public void fileWatcherFileNameFilter()
    {
        Predicate<String> filter = EnterpriseReadReplicaEditionModule.fileWatcherFileNameFilter();
        assertFalse( filter.test( MetaDataStore.DEFAULT_NAME ) );
        assertFalse( filter.test( StoreFile.NODE_STORE.fileName( StoreFileType.STORE ) ) );
        assertTrue( filter.test( PhysicalLogFile.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( IndexConfigStore.INDEX_DB_FILE_NAME + ".any" ) );
        assertTrue( filter.test( StoreUtil.BRANCH_SUBDIRECTORY ) );
        assertTrue( filter.test( StoreUtil.TEMP_COPY_DIRECTORY_NAME ) );
    }
}
