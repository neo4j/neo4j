/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * This test stress tests unique constraints in a setup where writes are being issued against a slave.
 */
public class UniqueConstraintStressIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule(getClass());

    protected ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.config(HaSettings.pull_interval, "0").startCluster( );
    }

    private final int REPETITIONS = 1;
    public final @Rule RepeatRule repeater = new RepeatRule();

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

    private abstract class Operation
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        Future<Boolean> constraintCreation = null;
        Future<Integer> constraintViolation = null;

        abstract void perform();
    }

    /* The different orders and delays in the below variations try to stress all known scenarios, as well as
     * of course stress for unknown concurrency issues. See the exception handling structure further below
     * for explanations. */

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_A() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );
                constraintViolation = slaveWork.execute( performInsert( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_B() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performInsert( slave ) );
                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_C() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );

                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                }

                constraintViolation = slaveWork.execute( performInsert( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_D() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performInsert( slave ) );

                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                }

                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_E() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                }

                constraintViolation = slaveWork.execute( performInsert( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat ( times = REPETITIONS )
    @Test
    public void shouldNotAllowUniquenessViolationsUnderStress_F() throws Exception
    {
        shouldNotAllowUniquenessViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performInsert( slave ) );

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                }

                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    public void shouldNotAllowUniquenessViolationsUnderStress(Operation ops) throws Exception
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
                ops.perform();

                // And then ensure all is well
                assertConstraintsNotViolated( ops.constraintCreation, ops.constraintViolation, master );
                successfulAttempts++;
            }
        }

        assertThat( successfulAttempts, greaterThan( 0 ) );
    }

    private void assertConstraintsNotViolated( Future<Boolean> constraintCreation, Future<Integer> constraintViolation,
                                               HighlyAvailableGraphDatabase master ) throws InterruptedException, ExecutionException
    {
        boolean constraintCreationFailed = constraintCreation.get();

        int txSuccessCount = constraintViolation.get();

        if ( constraintCreationFailed )
        {
            // Constraint creation failed, some of the violating operations should have been successful.
            assertThat( txSuccessCount, greaterThan( 0 ) );
            // And the graph should contain some duplicates.
            assertThat( allPropertiesAreUnique( master, label, property ), equalTo( false ) );
        }
        else
        {
            // Constraint creation was successful, all the violating operations should have failed.
            assertThat( txSuccessCount, equalTo( 0 ) );
            // And the graph should not contain duplicates.
            assertThat( allPropertiesAreUnique( master, label, property ), equalTo( true ) );
        }
    }

    private boolean allPropertiesAreUnique( HighlyAvailableGraphDatabase master, Label label, String property )
    {
        try( Transaction tx = master.beginTx() )
        {
            Set<Object> values = new HashSet<>();
            for ( Node node : loop( master.findNodes( label ) ) )
            {
                Object value = node.getProperty( property );
                if ( values.contains( value ) )
                {
                    return false;
                }
                values.add( value );
            }

            tx.success();
        }
        return true;
    }

    private OtherThreadExecutor.WorkerCommand<Object, Boolean> createConstraint( final HighlyAvailableGraphDatabase master )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Boolean>()
        {
            @Override
            public Boolean doWork( Object state ) throws Exception
            {
                boolean constraintCreationFailed = false;

                try( Transaction tx = master.beginTx() )
                {
                    master.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                    tx.success();
                }
                catch( ConstraintViolationException e )
                {
                    /* Unable to create constraint since it is not consistent with existing data. */
                    constraintCreationFailed = true;
                }

                return constraintCreationFailed;
            }
        };
    }

    /**
     * Inserts a bunch of new nodes with the label and property key currently set in the fields in this class, where
     * running this method twice will insert nodes with duplicate property values, assuming propertykey or label has not
     * changed.
     */
    private OtherThreadExecutor.WorkerCommand<Object, Integer> performInsert( final HighlyAvailableGraphDatabase slave )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Integer>()
        {
            @Override
            public Integer doWork( Object state ) throws Exception
            {
                int i = 0;

                try
                {
                    for ( ; i < 100; i++ )
                    {
                        try( Transaction tx = slave.beginTx() )
                        {
                            slave.createNode( label ).setProperty( property, "value-" + i );
                            tx.success();
                        }
                    }
                }
                catch ( TransactionFailureException e )
                {
                    // Swallowed on purpose, we except it to fail sometimes due to either
                    //  - constraint violation on master
                    //  - concurrent schema operation on master
                }
                catch ( ConstraintViolationException e )
                {
                    // Constraint violation detected on slave while building transaction
                }
                catch ( ComException e )
                {
                    // Happens sometimes, cause:
                    // - The lock session requested to start is already in use. Please retry your request in a few seconds.
                }
                return i;
            }
        };
    }

    private void setLabelAndPropertyForNextRound()
    {
        property = "Key-" + roundNo.incrementAndGet();
        label = DynamicLabel.label("Label-" + roundNo.get());
    }
}
