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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.helpers.collection.IteratorUtil.lastOrNull;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;
import static org.neo4j.kernel.impl.core.BigStoreIT.assertProperties;

@Ignore( "Ignored because the new page cache cannot turn off 'memory mapping', and that makes it run afoul of the " +
        "hack in JumpingFileSystemAbstraction$JumpingFileChannel.assertWithinDiff() for the PropertyStore alignment." )
public class BigJumpingStoreIT
{
    private static final int SIZE_PER_JUMP = 1000;
    private static final File PATH = new File( "target/var/bigjump" );
    private static final RelationshipType TYPE = DynamicRelationshipType.withName( "KNOWS" );
    private static final RelationshipType TYPE2 = DynamicRelationshipType.withName( "DROP_KICKS" );
    private GraphDatabaseService db;

    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
        db = new CommunityFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                    OperationalMode operationalMode)
            {
                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade, operationalMode )
                {
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        return new JumpingFileSystemAbstraction( SIZE_PER_JUMP );
                    }
                };
            }

            @Override
            protected EditionModule createEdition( PlatformModule platformModule )
            {
                return new CommunityEditionModule( platformModule )
                {
                    @Override
                    protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs,
                            IdTypeConfigurationProvider idTypeConfigurationProvider )
                    {
                        return new JumpingIdGeneratorFactory( SIZE_PER_JUMP );
                    }
                };
            }
        }.newFacade( PATH, config(), GraphDatabaseDependencies.newDependencies() );
    }

    private Map<String, String> config()
    {
        return stringMap( pagecache_memory.name(), "10M" );
    }

    @After
    public void doAfter()
    {
        if ( db != null )
        {
            db.shutdown();
        }
        db = null;
    }

    @Test
    public void crudOnHighIds() throws Exception
    {
        // Create stuff
        List<Node> nodes = new ArrayList<>();
        Transaction tx = db.beginTx();
        int numberOfNodes = SIZE_PER_JUMP * 3;
        String stringValue = "a longer string than short";
        byte[] arrayValue = new byte[]{3, 7};
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            Node node = db.createNode();
            node.setProperty( "number", i );
            node.setProperty( "string", stringValue );
            node.setProperty( "array", arrayValue );
            nodes.add( node );
        }

        int numberOfRels = numberOfNodes - 100;
        for ( int i = 0; i < numberOfRels; i++ )
        {
            Node node1 = nodes.get( i / 100 );
            Node node2 = nodes.get( i + 1 );
            Relationship rel = node1.createRelationshipTo( node2, TYPE );
        }

        tx.success();
        tx.close();

        // Verify
        tx = db.beginTx();
        int relCount = 0;
        for ( int t = 0; t < 2; t++ )
        {
            int nodeCount = 0;
            relCount = 0;
            for ( Node node : nodes )
            {
                node = db.getNodeById( node.getId() );
                assertProperties( map( "number", nodeCount++, "string", stringValue, "array", arrayValue ), node );
                relCount += count( node.getRelationships( Direction.OUTGOING ) );
            }
        }
        assertEquals( numberOfRels, relCount );
        tx.close();

        // Remove stuff
        tx = db.beginTx();
        for ( int i = 0; i < nodes.size(); i++ )
        {
            Node node = nodes.get( i );
            switch ( i % 6 )
            {
                case 0:
                    node.removeProperty( "number" );
                    break;
                case 1:
                    node.removeProperty( "string" );
                    break;
                case 2:
                    node.removeProperty( "array" );
                    break;
                case 3:
                    node.removeProperty( "number" );
                    node.removeProperty( "string" );
                    node.removeProperty( "array" );
                    break;
                case 4:
                    node.setProperty( "new", 34 );
                    break;
                case 5:
                    Object oldValue = node.getProperty( "string", null );
                    if ( oldValue != null )
                    {
                        node.setProperty( "string", "asjdkasdjkasjdkasjdkasdjkasdj" );
                        node.setProperty( "string", stringValue );
                    }
            }

            if ( count( node.getRelationships() ) > 50 )
            {
                if ( i % 2 == 0 )
                {
                    deleteIfNotNull( firstOrNull( node.getRelationships() ) );
                    deleteIfNotNull( lastOrNull( node.getRelationships() ) );
                }
                else
                {
                    deleteEveryOther( node.getRelationships() );
                }

                setPropertyOnAll( node.getRelationships( Direction.OUTGOING ), "relprop", "rel value" );
            }
            else if ( i % 20 == 0 )
            {
                Node otherNode = nodes.get( nodes.size() - i - 1 );
                Relationship rel = node.createRelationshipTo( otherNode, TYPE2 );
                rel.setProperty( "other relprop", 1010 );
            }
        }
        tx.success();
        tx.close();

        // Verify again
        tx = db.beginTx();
        for ( int t = 0; t < 2; t++ )
        {
            int nodeCount = 0;
            for ( Node node : nodes )
            {
                node = db.getNodeById( node.getId() );
                switch ( nodeCount % 6 )
                {
                    case 0:
                        assertProperties( map( "string", stringValue, "array", arrayValue ), node );
                        break;
                    case 1:
                        assertProperties( map( "number", nodeCount, "array", arrayValue ), node );
                        break;
                    case 2:
                        assertProperties( map( "number", nodeCount, "string", stringValue ), node );
                        break;
                    case 3:
                        assertEquals( 0, count( node.getPropertyKeys() ) );
                        break;
                    case 4:
                        assertProperties( map( "number", nodeCount, "string", stringValue, "array", arrayValue,
                                "new", 34 ), node );
                        break;
                    case 5:
                        assertProperties( map( "number", nodeCount, "string", stringValue, "array", arrayValue ),
                                node );
                        break;
                    default:
                }

                for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
                {
                    if ( rel.isType( TYPE ) )
                    {
                        assertProperties( map( "relprop", "rel value" ), rel );
                    }
                    else if ( rel.isType( TYPE2 ) )
                    {
                        assertProperties( map( "other relprop", 1010 ), rel );
                    }
                    else
                    {
                        fail( "Invalid type " + rel.getType() + " for " + rel );
                    }
                }
                nodeCount++;
            }
        }
        tx.close();
    }

    private void setPropertyOnAll( Iterable<Relationship> relationships, String key,
                                   Object value )
    {
        for ( Relationship rel : relationships )
        {
            rel.setProperty( key, value );
        }
    }

    private void deleteEveryOther( Iterable<Relationship> relationships )
    {
        int relCounter = 0;
        for ( Relationship rel : relationships )
        {
            if ( relCounter++ % 2 == 0 )
            {
                rel.delete();
            }
        }
    }

    private void deleteIfNotNull( Relationship relationship )
    {
        if ( relationship != null )
        {
            relationship.delete();
        }
    }
}
