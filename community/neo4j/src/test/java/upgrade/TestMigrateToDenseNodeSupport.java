/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package upgrade;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_store_upgrade;

public class TestMigrateToDenseNodeSupport
{
    private static final Label referenceNode = label( "ReferenceNode" );

    private enum Types implements RelationshipType
    {
        DENSE,
        SPARSE,
        OTHER,
        FOURTH
    }

    private enum Properties
    {
        BYTE( (byte) 10 ),
        SHORT( (short) 345 ),
        INT( 123456 ),
        LONG( 12345678901L ),
        CHAR( 'N' ),
        FLOAT( 123.456F ),
        DOUBLE( 123.456789D ),
        SHORT_STRING( "short" ),
        LONG_STRING( "super-duper long string that will have to spill over into a dynamic record !#¤%&/()=," ),
        INT_ARRAY( new int[]{12345, 67890, 123456789, 1, 2, 3, 4, 5} )
                {
                    @Override
                    public void assertValueEquals( Object otherValue )
                    {
                        assertArrayEquals( (int[]) value, (int[]) otherValue );
                    }
                },
        STRING_ARRAY( new String[]{"First", "Second", "Third", "Fourth"} )
                {
                    @Override
                    public void assertValueEquals( Object otherValue )
                    {
                        assertArrayEquals( (Object[]) value, (Object[]) otherValue );
                    }
                };

        protected final Object value;

        Properties( Object value )
        {
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }

        public void assertValueEquals( Object otherValue )
        {
            assertEquals( value, otherValue );
        }
    }

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    private File dir;

    @Before
    public void before() throws Exception
    {
        dir = Unzip.unzip( getClass(), "0.A.1-db.zip", testDir.graphDbDir() );
    }

    @Test
    public void migrateDbWithDenseNodes() throws Exception
    {
        // migrate
        new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( allow_store_upgrade, "true" ).newGraphDatabase().shutdown();

        // check consistency
        assertConsistentStore( dir );

        // open again to do extra checks
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir ).newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> allNodesWithLabel = db.findNodes( referenceNode );
            Node refNode = Iterators.single( allNodesWithLabel );
            int sparseCount = 0;
            for ( Relationship relationship : refNode.getRelationships( Types.SPARSE, OUTGOING ) )
            {
                verifySparseNode( db, relationship.getEndNode() );
                sparseCount++;
            }

            int denseCount = 0;
            for ( Relationship relationship : refNode.getRelationships( Types.DENSE, OUTGOING ) )
            {
                verifyDenseNode( db, relationship.getEndNode() );
                denseCount++;
            }

            assertEquals( 10, sparseCount );
            assertEquals( 10, denseCount );

            tx.success();
        }
        db.shutdown();
    }

    private void verifyDenseNode( GraphDatabaseService db, Node node )
    {
        assertEquals( 102, Iterables.count( node.getRelationships( OUTGOING ) ) );
        assertEquals( 100, Iterables.count( node.getRelationships( Types.OTHER, OUTGOING ) ) );
        assertEquals( 2, Iterables.count( node.getRelationships( Types.FOURTH, OUTGOING ) ) );
        verifyProperties( node );
        verifyDenseRepresentation( db, node, true );
    }

    private void verifySparseNode( GraphDatabaseService db, Node node )
    {
        assertEquals( 3, Iterables.count( node.getRelationships( OUTGOING ) ) );
        verifyProperties( node );
        verifyDenseRepresentation( db, node, false );
    }

    private void verifyProperties( Node node )
    {
        Set<String> keys = Iterables.asSet( node.getPropertyKeys() );
        for ( Properties property : Properties.values() )
        {
            assertTrue( keys.remove( property.name() ) );
            property.assertValueEquals( node.getProperty( property.name() ) );
        }
        assertTrue( keys.isEmpty() );
    }

    private void verifyDenseRepresentation( GraphDatabaseService db, Node node, boolean dense )
    {
        KernelAPI kernelAPI = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( KernelAPI.class );
        try ( KernelTransaction tx = kernelAPI.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
              Statement statement = tx.acquireStatement() )
        {
            Cursor<NodeItem> nodeCursor = statement.readOperations().nodeCursor( node.getId() );
            assertTrue( nodeCursor.next() );
            assertEquals( dense, nodeCursor.get().isDense() );
        }
        catch ( TransactionFailureException | IllegalArgumentException e )
        {
            throw new RuntimeException( e );
        }
    }

}
