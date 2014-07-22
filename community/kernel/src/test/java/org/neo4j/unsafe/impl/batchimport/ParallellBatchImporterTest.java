/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.unsafe.impl.batchimport.cache.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.DetailedExecutionMonitor;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ParallellBatchImporterTest
{
    private static final long seed = 12345L;

    @Test
    public void shouldImportCsvData() throws Exception
    {

        // GIVEN
        Configuration config = new Configuration.Default()
        {
            @Override
            public int denseNodeThreshold()
            {
                return 30;
            }
        };
        BatchImporter inserter = new ParallellBatchImporter( directory.getAbsolutePath(),
                new DefaultFileSystemAbstraction(), config,
                Iterables.<KernelExtensionFactory<?>>empty(), new DetailedExecutionMonitor() );

        // WHEN
        int nodeCount = 100_000;
        int relationshipCount = nodeCount*10;
        inserter.doImport( nodes( nodeCount ), relationships( relationshipCount, nodeCount ), IdMappers.actualIds() );
        inserter.shutdown();

        // THEN
        System.out.println( "Verifying contents" );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.getAbsolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            Random random = new Random();
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.getNodeById( random.nextInt( nodeCount ) );
                int count = count( node.getRelationships() );
                assertEquals( "For node " + node, count, node.getDegree() );
                for ( String key : node.getPropertyKeys() )
                {
                    node.getProperty( key );
                }
            }

            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private Iterable<InputRelationship> relationships( final long count, final long maxNodeId )
    {
        return new Iterable<InputRelationship>()
        {
            @Override
            public Iterator<InputRelationship> iterator()
            {
                return new PrefetchingIterator<InputRelationship>()
                {
                    private final Random random = new Random( seed );
                    private int cursor = 0;
                    private final Object[] properties = new Object[] {
                            "name", "Nisse " + count,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic record1234567890!@#$%^&*()_|",
                            "array", new long[] { 1234567890123L, 987654321987L, 123456789123L, 987654321987L }
                            };

                    @Override
                    protected InputRelationship fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            try
                            {
                                return new InputRelationship( cursor, properties, null,
                                        Math.abs( random.nextLong()%maxNodeId ),
                                        Math.abs( random.nextLong()%maxNodeId ), "TYPE" + random.nextInt( 3 ), null );
                            }
                            finally
                            {
                                cursor++;
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }

    private Iterable<InputNode> nodes( final long count )
    {
        return new Iterable<InputNode>()
        {
            @Override
            public Iterator<InputNode> iterator()
            {
                return new PrefetchingIterator<InputNode>()
                {
                    private int cursor = 0;
                    private final Object[] properties = new Object[] {
                            "name", "Nisse " + count,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic record1234567890!@#$%^&*()_|",
                            "array", new long[] { 1234567890123L, 987654321987L, 123456789123L, 987654321987L }
                            };
                    private final String[] labels = new String[] { "Person", "Guy" };

                    @Override
                    protected InputNode fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            try
                            {
                                return new InputNode( cursor, properties, null, labels, null );
                            }
                            finally
                            {
                                cursor++;
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }

    public final File directory = TargetDirectory.forTest( getClass() ).cleanDirectory( "import" );
}
