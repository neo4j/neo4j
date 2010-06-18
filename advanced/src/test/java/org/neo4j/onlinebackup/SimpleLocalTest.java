/*
 * Copyright (c) 2009-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Test to backup only Neo4j to a backup location.
 */
public class SimpleLocalTest extends SimpleRunningTest
{
    @Override
    protected void tryBackup( EmbeddedGraphDatabase graphDb,
            String location, int relCount ) throws IOException
    {
        System.out.println( "backing up to backup location" );
        Backup backupComp = Neo4jBackup.neo4jDataSource( graphDb, location );
        backupComp.doBackup();
        EmbeddedGraphDatabase bDb = Util.startGraphDbInstance( location );
        Transaction bTx = bDb.beginTx();
        try
        {
            List<Relationship> rels = new ArrayList<Relationship>();
            for ( Relationship rel : bDb.getReferenceNode().getRelationships() )
            {
                rels.add( rel );
            }
            assertEquals( relCount, rels.size() );
            bTx.success();
        }
        finally
        {
            bTx.finish();
        }
        Util.stopGraphDb( bDb );
    }
}
