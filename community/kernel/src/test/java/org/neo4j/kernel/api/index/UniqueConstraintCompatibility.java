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
package org.neo4j.kernel.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.single;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class UniqueConstraintCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public UniqueConstraintCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    /*
     * There are a quite a number of permutations to consider, when it comes to unique
     * constraints.
     *
     * We have two supported providers:
     *  - InMemoryIndexProvider
     *  - LuceneSchemaIndexProvider
     *
     * An index can be in a number of states, two of which are interesting:
     *  - ONLINE: the index is in active duty
     *  - POPULATING: the index is in the process of being created and filled with data
     *
     * Further more, indexes that are POPULATING have two ways of injesting data:
     *  - Through add()'ing existing data
     *  - Through NodePropertyUpdates sent to a "populating udpater"
     *
     * Then, when we add data to an index, two outcomes are possible, depending on the
     * data:
     *  - The index does not contain an equivalent value, and the entity id is added to
     *    the index.
     *  - The index already contains an equivalent value, and the addition is rejected.
     *
     * And when it comes to observing these outcomes, there are a whole bunch of
     * interesting transaction states that are worth exploring:
     *  - Adding a label to a node
     *  - Removing a label from a node
     *  - Combinations of adding and removing a label, ultimately adding it
     *  - Combinations of adding and removing a label, ultimately removing it
     *  - Adding a property
     *  - Removing a property
     *  - Changing an existing property
     *  - Combinations of adding and removing a property, ultimately adding it
     *  - Combinations of adding and removing a property, ultimately removing it
     *  - Likewise combinations of adding, removing and changing a property
     *
     * To make matters worse, we index a number of different types, some of which may or
     * may not collide in the index because of coercion. We need to make sure that the
     * indexes deal with these values correctly. And we also have the ways in which these
     * operations can be performed in any number of transactions, for instance, if all
     * the conflicting nodes were added in the same transaction or not.
     *
     * All in all, we have many cases to test for!
     *
     * Still, it is possible to boild things down a little bit, because there are fewer
     * outcomes than there are scenarios that lead to those outcomes. With a bit of
     * luck, we can abstract over the scenarios that lead to those outcomes, and then
     * only write a test per outcome. These are the outcomes I see:
     *  - Populating an index succeeds
     *  - Populating an index fails because of the existing data
     *  - Populating an index fails because of updates to data
     *  - Adding to an online index succeeds
     *  - Adding to an online index fails because of existing data
     *  - Adding to an online index fails because of data in the same transaction
     *
     * There's a lot of work to be done here.
     */

    // -- Tests:

    @Test
    public void shouldAcceptDistinctValuesInDifferentTransactionsWhenOnline()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
        Node a, b;
        Object va = "a", vb = "b";
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode( label );
            a.setProperty( property, va );
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            b = db.createNode( label );
            b.setProperty( property, vb );
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( single( db.findNodesByLabelAndProperty( label, property, va ) ).getProperty( property ),
                    is( va ) );
            assertThat( single( db.findNodesByLabelAndProperty( label, property, vb ) ).getProperty( property ),
                    is( vb ) );
            tx.success();
        }
    }

    // TODO equiv. of UniqueIAC: shouldConsiderWholeTransactionForValidatingUniqueness
    // TODO equiv. of UniqueIAC: shouldRejectChangingEntryToAlreadyIndexedValue
    // TODO equiv. of UniqueIAC: shouldRemoveAndAddEntries
    // TODO equiv. of UniqueIAC: shouldRejectAddingEntryToValueAlreadyIndexedByPriorChange
    // TODO equiv. of UniqueIAC: shouldRejectEntryWithAlreadyIndexedValue
    // TODO equiv. of UniqueIAC: shouldAddUniqueEntries
    // TODO equiv. of UniqueIAC: shoouldRejectEntriesInSameTransactionWithDuplicateIndexedValue
    // TODO equiv. of UniqueIAC: shouldUpdateUniqueEntries
    // TODO equiv. of UniqueIPC: should*EnforceUniqueConstraintsAgainstDataAddedOnline
    // TODO equiv. of UniqueIPC: should*EnforceUniqueConstraints
    // TODO equiv. of UniqueIPC: should*EnforceUniqueConstraintsAgainstDataAddedThroughPopulator
    // TODO equiv. of UniqueIPC: should*EnforceUnqieConstraintsAgainstDataAddedInSameTx

    // TODO equiv. of UniqueLucIAT: shouldRejectChangingEntryToAlreadyIndexedValue
    // TODO equiv. of UniqueLucIAT: shouldRejectAddingEntryToValueAlreadyIndexedByPriorChange
    // TODO equiv. of UniqueLucIAT: shouldRejectEntryWithAlreadyIndexedValue
    // TODO equiv. of UniqueLucIAT: shouldRejectEntriesInSameTransactionWithDuplicatedIndexedValues

    @Test
    public void shouldAcceptDistinctValuesInSameTransactionsWhenOnline()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
        Node a, b;
        Object va = "a", vb = "b";

        // When
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode( label );
            a.setProperty( property, va );

            b = db.createNode( label );
            b.setProperty( property, vb );
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( single( db.findNodesByLabelAndProperty( label, property, va ) ).getProperty( property ),
                    is( va ) );
            assertThat( single( db.findNodesByLabelAndProperty( label, property, vb ) ).getProperty( property ),
                    is( vb ) );
            tx.success();
        }
    }

    @Test
    public void shouldNotFalselyCollideOnFindNodesByLabelAndProperty() throws Exception
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
        Node a, b;
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode( label );
            a.setProperty( property, 4611686018427387905L );
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            b = db.createNode( label );
            b.setProperty( property, 4611686018427387907L );
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( single( db.findNodesByLabelAndProperty( label, property, 4611686018427387905L ) ), is( a ) );
            assertThat( single( db.findNodesByLabelAndProperty( label, property, 4611686018427387907L ) ), is( b ) );
            tx.success();
        }
    }




    // -- Set Up:

    private Label label = DynamicLabel.label( "Cybermen" );
    private String property = "name";

    private GraphDatabaseService db;

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.cleanTestDirForTest( getClass() );

    @Before
    public void setUp() {
        String storeDir = testDirectory.absolutePath();
        TestGraphDatabaseFactory dbfactory = new TestGraphDatabaseFactory();
        dbfactory.addKernelExtension( new PredefinedSchemaIndexProviderFactory( indexProvider ) );
        db = dbfactory.newImpermanentDatabase( storeDir );
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    private static class PredefinedSchemaIndexProviderFactory extends KernelExtensionFactory<PredefinedSchemaIndexProviderFactory.NoDeps>
    {
        private final SchemaIndexProvider indexProvider;

        @Override
        public Lifecycle newKernelExtension( NoDeps noDeps ) throws Throwable
        {
            return indexProvider;
        }

        public static interface NoDeps {
        }

        public PredefinedSchemaIndexProviderFactory( SchemaIndexProvider indexProvider )
        {
            super( indexProvider.getClass().getSimpleName() );
            this.indexProvider = indexProvider;
        }
    }
}
