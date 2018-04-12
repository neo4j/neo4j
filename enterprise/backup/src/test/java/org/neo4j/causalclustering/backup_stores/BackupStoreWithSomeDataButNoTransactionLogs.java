/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.backup_stores;

import java.io.File;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;

import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.helpers.DataCreator.createEmptyNodes;

public class BackupStoreWithSomeDataButNoTransactionLogs extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster cluster ) throws Exception
    {
        return createEmptyNodes( cluster, 10 );
    }

    @Override
    void modify( File backup )
    {
        for ( File transaction : backup.listFiles( ( dir, name ) -> name.contains( "transaction" ) ) )
        {
            assertTrue( transaction.delete() );
        }
    }
}
