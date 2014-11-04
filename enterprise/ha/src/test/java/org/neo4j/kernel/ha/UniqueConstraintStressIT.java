/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.com.ComException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

/**
 * This test stress tests unique constraints in a setup where writes are being issued against a slave.
 */
public class UniqueConstraintStressIT extends AbstractClusterTest
{
    @Before
    public void setUp()
    {
        cluster.await( allSeesAllAsAvailable() );
    }

    @Rule
    public OtherThreadRule<Object> slaveWork = new OtherThreadRule<>();

    @Rule
    public OtherThreadRule<Object> masterWork = new OtherThreadRule<>();

    /** Configure how long to run the test. */
    private static long runtime = Long.getLong( "neo4j.UniqueConstraintStressIT.runtime", TimeUnit.SECONDS.toMillis( 10 ) );

    /** Label to constrain for the current iteration of the test. */
    private volatile Label label = DynamicLabel.label( "User" );

    /** Property key to constrain for the current iteration of the test. */
    private volatile String property;

    private final AtomicInteger roundNo = new AtomicInteger(0);

    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress() throws Exception
    {
        // Given
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // Because this is a brute-force test, we run for a user-specified time, and consider ourselves successful if no failure is found.
        long end = System.currentTimeMillis() + runtime;
        int successfulAttempts = 0;
        while ( end > System.currentTimeMillis() )
        {
            // Each round of this loop:
            //  - on a slave, creates a set of nodes with a label/property combo with all-unique values
            //  - on the master, creates a unique constraint
            //  - on a slave, while the master is creating the constraint, starts issuing transactions that will violate the constraint

            setup :
            {
                // Set a target property for the constraint for this round
                setLabelAndPropertyForNextRound();

                // Ensure slave has constraints from previous round
                cluster.sync();

                // Create the initial data
                try
                {
                    slaveWork.execute( performInsert( slave ) ).get();
                }
                catch ( ExecutionException e )
                {
                    if ( Exceptions.contains( e, "could not connect", ComException.class ) )
                    {   // We're in a weird cluster state. The slave should be able to succeed if trying again
                        continue;
                    }
                }
            }

            stress :
            {
                // Create the constraint on the master
                Future<Object> constraintCreation = masterWork.execute( createConstraint( master ) );

                // Concurrently start violating the data via the slave
                Future<Object> constraintViolation = slaveWork.execute( performInsert( slave ) );

                // And then ensure all is well
                assertConstraintsNotViolated( constraintCreation, constraintViolation, master );
                successfulAttempts++;
            }
        }

        assertThat( successfulAttempts, greaterThan( 0 ) );
    }

    private void assertConstraintsNotViolated( Future<Object> constraintCreation, Future<Object> constraintViolation,
                                               HighlyAvailableGraphDatabase master ) throws InterruptedException, ExecutionException
    {
        try
        {
            constraintCreation.get();
        }
        catch(ExecutionException e)
        {
            assertThat(e.getCause(), instanceOf( ConstraintViolationException.class));

            // Constraint creation failed. Then the violating operations should have been successful
            constraintViolation.get();
        }

        // If the constraint creation was successful, the violating operations should have failed, and the graph
        // should not contain duplicates
        try
        {
            constraintViolation.get();
            fail( "Constraint violating operations succeeded, even though they were violating a successfully created constraint." );
        }
        catch(ExecutionException e)
        {
            assertThat(e.getCause(), instanceOf( TransactionFailureException.class));
            assertAllPropertiesAreUnique( master, label, property );
        }
    }

    private void assertAllPropertiesAreUnique( HighlyAvailableGraphDatabase master, Label label, String property )
    {
        try( Transaction tx = master.beginTx() )
        {
            Set<Object> values = new HashSet<>();
            for ( Node node : loop( master.findNodes( label ) ) )
            {
                Object value = node.getProperty( property );
                if ( values.contains( value ) )
                {
                    throw new AssertionError( label + ":" + property + " contained duplicate property value '" + value + "', despite constraint existing." );
                }
                values.add( value );
            }

            tx.success();
        }
    }

    private OtherThreadExecutor.WorkerCommand<Object, Object> createConstraint( final HighlyAvailableGraphDatabase master )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Object>()
        {
            @Override
            public Object doWork( Object state ) throws Exception
            {
                try( Transaction tx = master.beginTx() )
                {
                    master.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                    tx.success();
                }
                return null;
            }
        };
    }

    /**
     * Inserts a bunch of new nodes with the label and property key currently set in the fields in this class, where
     * running this method twice will insert nodes with duplicate property values, assuming propertykey or label has not
     * changed.
     */
    private OtherThreadExecutor.WorkerCommand<Object, Object> performInsert( final HighlyAvailableGraphDatabase slave )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Object>()
        {
            @Override
            public Object doWork( Object state ) throws Exception
            {
                for ( int i = 0; i < 1000; i++ )
                {
                    try( Transaction tx = slave.beginTx() )
                    {
                        slave.createNode( label ).setProperty( property, "value-" + i );
                        tx.success();
                    }
                }
                return null;
            }
        };
    }

    private void setLabelAndPropertyForNextRound()
    {
        property = "Key-" + roundNo.incrementAndGet();
        label = DynamicLabel.label("Label-" + roundNo.get());
    }
}
