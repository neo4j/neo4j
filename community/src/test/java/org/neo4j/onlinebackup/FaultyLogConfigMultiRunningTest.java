/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.onlinebackup;

import java.io.IOException;

import org.junit.Test;
import org.neo4j.index.lucene.LuceneDataSource;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Sets keep_logical_logs to false, so an exception is thrown.
 */
public class FaultyLogConfigMultiRunningTest extends MultiRunningTest
{

    @Override
    protected void configureSourceDb( final EmbeddedGraphDatabase graphDb )
    {
        XaDataSource neoStoreXaDataSource = graphDb.getConfig().getPersistenceModule().getPersistenceManager().getPersistenceSource().getXaDataSource();
        neoStoreXaDataSource.keepLogicalLogs( false );
        XaDataSourceManager xaDsm = graphDb.getConfig().getTxModule().getXaDataSourceManager();
        XaDataSource ds = xaDsm.getXaDataSource( "lucene" );
        ( (LuceneDataSource) ds ).keepLogicalLogs( false );
    }

    @Override
    @Test( expected = IllegalStateException.class )
    public void backup() throws IOException
    {
        super.backup();
    }
}
