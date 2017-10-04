/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.recovery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProviderFactory;
import org.neo4j.kernel.api.impl.schema.NativeLuceneFusionSchemaIndexProviderFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.Label.label;

@RunWith( Parameterized.class )
public class UniqueIndexRecoveryTest
{
    @Rule
    public final TestDirectory storeDir = TestDirectory.testDirectory();

    private static final String PROPERTY_KEY = "key";
    private static final String PROPERTY_VALUE = "value";
    private static final Label LABEL = label( "label" );

    private final TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
    private GraphDatabaseAPI db;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> parameters()
    {
        return asList(
                new Object[]{new LuceneSchemaIndexProviderFactory()},
                new Object[]{new NativeLuceneFusionSchemaIndexProviderFactory()},
                new Object[]{new InMemoryIndexProviderFactory()} );
    }

    @Parameterized.Parameter( 0 )
    public KernelExtensionFactory<?> kernelExtensionFactory;

    @Before
    public void before()
    {
        List<KernelExtensionFactory<?>> extensionFactories = new ArrayList<>();
        extensionFactories.add( kernelExtensionFactory );
        factory.setKernelExtensions( extensionFactories );
        db = newDb();
    }

    private GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) factory
                .newEmbeddedDatabaseBuilder( storeDir.absolutePath() )
                .newGraphDatabase();
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    @Test
    public void shouldRecoverCreationOfUniquenessConstraintFollowedByDeletionOfThatSameConstraint() throws Exception
    {
        // given
        createUniqueConstraint();
        dropConstraints();

        // when - perform recovery
        restart( snapshot( storeDir.absolutePath() ) );

        // then - just make sure the constraint is gone
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.schema().getConstraints( LABEL ).iterator().hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldRecoverWhenCommandsTemporarilyViolateConstraints() throws Exception
    {
        // GIVEN
        Node unLabeledNode = createUnLabeledNodeWithProperty();
        Node labeledNode = createLabeledNode();
        createUniqueConstraint();
        rotateLogAndCheckPoint(); // snapshot
        setPropertyOnLabeledNode( labeledNode );
        deletePropertyOnLabeledNode( labeledNode );
        addLabelToUnLabeledNode( unLabeledNode );
        flushAll(); // persist - recovery will do everything since last log rotate

        // WHEN recovery is triggered
        restart( snapshot( storeDir.absolutePath() ) );

        // THEN
        // it should just not blow up!
        try ( Transaction tx = db.beginTx() )
        {
            assertThat(
                    db.findNode( LABEL, PROPERTY_KEY, PROPERTY_VALUE ),
                    equalTo( unLabeledNode ) );
            tx.success();
        }
    }

    private void restart( File newStore )
    {
        db.shutdown();
        db = newDb();
    }

    private File snapshot( final File path ) throws IOException
    {
        File snapshotDir = new File( path, "snapshot-" + new Random().nextInt() );
        FileUtils.copyRecursively( path, snapshotDir, pathName ->
        {
            String subPath = pathName.getAbsolutePath().substring( path.getPath().length() ).replace( File.separatorChar, '/' );
            if ( "/store_lock".equals( subPath ) )
            {
                return false; // since the db is running, exclude the 'store_lock' file
            }
            if ( subPath.startsWith( "/schema/index/lucene/" ) || subPath.startsWith( "/schema/label/lucene/" ) )
            {
                return !subPath.endsWith( "/write.lock" ); // since the db is running, exclude lucene lock files
            }
            return true;
        } );
        return snapshotDir;
    }

    private void addLabelToUnLabeledNode( Node unLabeledNode )
    {
        try ( Transaction tx = db.beginTx() )
        {
            unLabeledNode.addLabel( LABEL );
            tx.success();
        }
    }

    private void setPropertyOnLabeledNode( Node labeledNode )
    {
        try ( Transaction tx = db.beginTx() )
        {
            labeledNode.setProperty( PROPERTY_KEY, PROPERTY_VALUE );
            tx.success();
        }
    }

    private void deletePropertyOnLabeledNode( Node labeledNode )
    {
        try ( Transaction tx = db.beginTx() )
        {
            labeledNode.removeProperty( PROPERTY_KEY );
            tx.success();
        }
    }

    private void createUniqueConstraint()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.success();
        }
    }

    private Node createLabeledNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            tx.success();
            return node;
        }
    }

    private Node createUnLabeledNodeWithProperty()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( PROPERTY_KEY, PROPERTY_VALUE );
            tx.success();
            return node;
        }
    }

    private void dropConstraints()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints( LABEL ) )
            {
                constraint.drop();
            }
            tx.success();
        }
    }

    private void rotateLogAndCheckPoint() throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private void flushAll()
    {
        db.getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( IOLimiter.unlimited() );
    }
}
