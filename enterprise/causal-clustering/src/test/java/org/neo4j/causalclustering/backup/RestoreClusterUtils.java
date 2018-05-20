/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.backup;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

public class RestoreClusterUtils
{
    private RestoreClusterUtils()
    {
    }

    public static File createClassicNeo4jStore( File base, FileSystemAbstraction fileSystem, int nodesToCreate,
            String recordFormat )
    {
        return createClassicNeo4jStore( base, fileSystem, nodesToCreate, recordFormat, StringUtils.EMPTY );
    }

    public static File createClassicNeo4jStore( File base, FileSystemAbstraction fileSystem,
            int nodesToCreate, String recordFormat, String logicalLogsLocation )
    {
        File storeDir = new File( base, "existing" );
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fileSystem )
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .setConfig( GraphDatabaseSettings.logical_logs_location, logicalLogsLocation )
                .newGraphDatabase();

        for ( int i = 0; i < (nodesToCreate / 2); i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode( Label.label( "Label-" + i ) );
                Node node2 = db.createNode( Label.label( "Label-" + i ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                tx.success();
            }
        }

        db.shutdown();

        return storeDir;
    }
}
