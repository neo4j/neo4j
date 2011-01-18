/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestMigration
{
    @Test
    public void canReadAndUpgradeOldIndexStoreFormat() throws Exception
    {
        String path = "target/var/old-index-store";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        db.shutdown();
        InputStream stream = getClass().getClassLoader().getResourceAsStream( "old-index.db" );
        writeFile( stream, new File( path, "index.db" ) );
        db = new EmbeddedGraphDatabase( path );
        assertTrue( db.index().existsForNodes( "indexOne" ) );
        Index<Node> indexOne = db.index().forNodes( "indexOne" );
        verifyConfiguration( db, indexOne, LuceneIndexProvider.EXACT_CONFIG );
        assertTrue( db.index().existsForNodes( "indexTwo" ) );
        Index<Node> indexTwo = db.index().forNodes( "indexTwo" );
        verifyConfiguration( db, indexTwo, LuceneIndexProvider.FULLTEXT_CONFIG );
        assertTrue( db.index().existsForRelationships( "indexThree" ) );
        Index<Relationship> indexThree = db.index().forRelationships( "indexThree" );
        verifyConfiguration( db, indexThree, LuceneIndexProvider.EXACT_CONFIG );
        db.shutdown();
    }

    private void verifyConfiguration( GraphDatabaseService db, Index<? extends PropertyContainer> index, Map<String, String> config )
    {
        assertEquals( config, db.index().getConfiguration( index ) );
    }

    private void writeFile( InputStream stream, File file ) throws Exception
    {
        file.delete();
        OutputStream out = new FileOutputStream( file );
        byte[] bytes = new byte[1024];
        int bytesRead = 0;
        while ( (bytesRead = stream.read( bytes )) >= 0 )
        {
            out.write( bytes, 0, bytesRead );
        }
        out.close();
    }
}
