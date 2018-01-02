/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.ComException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import static java.lang.String.format;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * This test stress tests unique constraints in a setup where writes are being issued against a slave.
 */
@RunWith( Parameterized.class )
public class PropertyConstraintsStressIT
{
    @Parameterized.Parameter
    public ConstraintOperations constraintOps;

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Rule
    public OtherThreadRule<Object> slaveWork = new OtherThreadRule<>();

    @Rule
    public OtherThreadRule<Object> masterWork = new OtherThreadRule<>();

    @Rule
    public RepeatRule repeater = new RepeatRule();

    protected ClusterManager.ManagedCluster cluster;

    private final int REPETITIONS = 1;
    /** Configure how long to run the test. */
    private static long runtime =
            Long.getLong( "neo4j.PropertyConstraintsStressIT.runtime", TimeUnit.SECONDS.toMillis( 10 ) );

    /** Label or relationship type to constrain for the current iteration of the test. */
    private volatile String labelOrRelType = "Foo";

    /** Property key to constrain for the current iteration of the test. */
    private volatile String property;

    private final AtomicInteger roundNo = new AtomicInteger( 0 );

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> params()
    {
        return Arrays.asList( new Object[][]{
                {UNIQUE_PROPERTY_CONSTRAINT_OPS},
                {NODE_PROPERTY_EXISTENCE_CONSTRAINT_OPS},
                {REL_PROPERTY_EXISTENCE_CONSTRAINT_OPS},
        } );
    }

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.withSharedSetting( HaSettings.pull_interval, "0" ).startCluster();
    }

    /* The different orders and delays in the below variations try to stress all known scenarios, as well as
     * of course stress for unknown concurrency issues. See the exception handling structure further below
     * for explanations. */

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_A() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );
                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_B() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );
                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_C() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );

                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException ignore )
                {
                }

                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_D() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );

                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException ignore )
                {
                }

                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_E() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintCreation = masterWork.execute( createConstraint( master ) );

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException ignore )
                {
                }

                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );
            }
        } );
    }

    @RepeatRule.Repeat( times = REPETITIONS )
    @Test
    public void shouldNotAllowConstraintsViolationsUnderStress_F() throws Exception
    {
        shouldNotAllowConstraintsViolationsUnderStress( new Operation()
        {
            @Override
            void perform()
            {
                constraintViolation = slaveWork.execute( performConstraintViolatingInserts( slave ) );

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException ignore )
                {
                }

                constraintCreation = masterWork.execute( createConstraint( master ) );
            }
        } );
    }

    public void shouldNotAllowConstraintsViolationsUnderStress( Operation ops ) throws Exception
    {
        // Given
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // Because this is a brute-force test, we run for a user-specified time, and consider ourselves successful if
        // no failure is found.
        long end = System.currentTimeMillis() + runtime;
        int successfulAttempts = 0;
        while ( end > System.currentTimeMillis() )
        {
            // Each round of this loop:
            //  - on a slave, creates a set of nodes with a label/property combo with all-unique values
            //  - on the master, creates a property constraint
            //  - on a slave, while the master is creating the constraint, starts transactions that will
            //    violate the constraint

            //setup :
            {
                // Set a target property for the constraint for this round
                setLabelAndPropertyForNextRound();

                // Ensure slave has constraints from previous round
                cluster.sync();

                // Create the initial data that *does not* violate the constraint
                try
                {
                    slaveWork.execute( performConstraintCompliantInserts( slave ) ).get();
                }
                catch ( ExecutionException e )
                {
                    if ( Exceptions.contains( e, "could not connect", ComException.class ) )
                    {   // We're in a weird cluster state. The slave should be able to succeed if trying again
                        continue;
                    }
                }
            }

            //stress :
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
            assertThat( constraintOps.isValid( master, labelOrRelType, property ), equalTo( false ) );
        }
        else
        {
            // Constraint creation was successful, all the violating operations should have failed.
            assertThat( txSuccessCount, equalTo( 0 ) );
            // And the graph should not contain duplicates.
            assertThat( constraintOps.isValid( master, labelOrRelType, property ), equalTo( true ) );
        }
    }

    private WorkerCommand<Object,Boolean> createConstraint( final HighlyAvailableGraphDatabase master )
    {
        return constraintOps.createConstraint( master, labelOrRelType, property );
    }

    private WorkerCommand<Object,Integer> performConstraintCompliantInserts( HighlyAvailableGraphDatabase slave )
    {
        return performInserts( slave, true );
    }

    private WorkerCommand<Object,Integer> performConstraintViolatingInserts( HighlyAvailableGraphDatabase slave )
    {
        return performInserts( slave, false );
    }

    /**
     * Inserts a bunch of new nodes with the label and property key currently set in the fields in this class, where
     * running this method twice will insert nodes with duplicate property values, assuming property key or label has
     * not changed.
     */
    private WorkerCommand<Object,Integer> performInserts( final HighlyAvailableGraphDatabase slave,
            final boolean constraintCompliant )
    {
        return new WorkerCommand<Object,Integer>()
        {
            @Override
            public Integer doWork( Object state ) throws Exception
            {
                int i = 0;

                try
                {
                    for (; i < 100; i++ )
                    {
                        try ( Transaction tx = slave.beginTx() )
                        {
                            constraintOps.createEntity( slave, labelOrRelType, property, "value" + i,
                                    constraintCompliant );
                            tx.success();
                        }
                    }
                }
                catch ( TransactionFailureException | TransientTransactionFailureException e )
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
                    // - The lock session requested to start is already in use.
                    //   Please retry your request in a few seconds.
                }
                return i;
            }
        };
    }

    private void setLabelAndPropertyForNextRound()
    {
        property = "Key-" + roundNo.incrementAndGet();
        labelOrRelType = "Foo-" + roundNo.get();
    }

    private abstract class Operation
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        Future<Boolean> constraintCreation = null;
        Future<Integer> constraintViolation = null;

        abstract void perform();
    }

    interface ConstraintOperations
    {
        void createEntity( HighlyAvailableGraphDatabase db, String type, String propertyKey, Object value,
                boolean constraintCompliant );

        boolean isValid( HighlyAvailableGraphDatabase master, String type, String property );

        WorkerCommand<Object,Boolean> createConstraint( HighlyAvailableGraphDatabase db, String type, String property );
    }

    private static final ConstraintOperations UNIQUE_PROPERTY_CONSTRAINT_OPS = new ConstraintOperations()
    {
        @Override
        public void createEntity( HighlyAvailableGraphDatabase db, String type, String propertyKey, Object value,
                boolean constraintComplient )
        {
            db.createNode( label( type ) ).setProperty( propertyKey, value );
        }

        @Override
        public boolean isValid( HighlyAvailableGraphDatabase db, String type, String property )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Set<Object> values = new HashSet<>();
                for ( Node node : loop( db.findNodes( label( type ) ) ) )
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

        @Override
        public WorkerCommand<Object,Boolean> createConstraint( final HighlyAvailableGraphDatabase db, final String type,
                final String property )
        {
            return new WorkerCommand<Object,Boolean>()
            {
                @Override
                public Boolean doWork( Object state ) throws Exception
                {
                    boolean constraintCreationFailed = false;

                    try ( Transaction tx = db.beginTx() )
                    {
                        db.schema().constraintFor( label( type ) ).assertPropertyIsUnique( property ).create();
                        tx.success();
                    }
                    catch ( ConstraintViolationException e )
                    {
                    /* Unable to create constraint since it is not consistent with existing data. */
                        constraintCreationFailed = true;
                    }

                    return constraintCreationFailed;
                }
            };
        }

        @Override
        public String toString()
        {
            return "UNIQUE_PROPERTY_CONSTRAINT";
        }
    };

    private static final ConstraintOperations NODE_PROPERTY_EXISTENCE_CONSTRAINT_OPS = new ConstraintOperations()
    {
        @Override
        public void createEntity( HighlyAvailableGraphDatabase db, String type, String propertyKey, Object value,
                boolean constraintCompliant )
        {
            Node node = db.createNode( label( type ) );
            if ( constraintCompliant )
            {
                node.setProperty( propertyKey, value );
            }
        }

        @Override
        public boolean isValid( HighlyAvailableGraphDatabase db, String type, String property )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( Node node : loop( db.findNodes( label( type ) ) ) )
                {
                    if ( !node.hasProperty( property ) )
                    {
                        return false;
                    }
                }

                tx.success();
            }
            return true;
        }

        @Override
        public WorkerCommand<Object,Boolean> createConstraint( HighlyAvailableGraphDatabase db, String type,
                String property )
        {
            String query = format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", type, property );
            return createPropertyExistenceConstraintCommand( db, query );
        }

        @Override
        public String toString()
        {
            return "NODE_PROPERTY_EXISTENCE_CONSTRAINT";
        }
    };

    private static final ConstraintOperations REL_PROPERTY_EXISTENCE_CONSTRAINT_OPS = new ConstraintOperations()
    {
        @Override
        public void createEntity( HighlyAvailableGraphDatabase db, String type, String propertyKey, Object value,
                boolean constraintCompliant )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            Relationship relationship = start.createRelationshipTo( end, withName( type ) );
            if ( constraintCompliant )
            {
                relationship.setProperty( propertyKey, value );
            }
        }

        @Override
        public boolean isValid( HighlyAvailableGraphDatabase db, String type, String property )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
                {
                    if ( relationship.getType().name().equals( type ) )
                    {
                        if ( !relationship.hasProperty( property ) )
                        {
                            return false;
                        }
                    }
                }
                tx.success();
            }
            return true;
        }

        @Override
        public WorkerCommand<Object,Boolean> createConstraint( HighlyAvailableGraphDatabase db, String type,
                String property )
        {
            String query = format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, property );
            return createPropertyExistenceConstraintCommand( db, query );
        }

        @Override
        public String toString()
        {
            return "RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT";
        }
    };

    private static WorkerCommand<Object,Boolean> createPropertyExistenceConstraintCommand(
            final GraphDatabaseService db, final String query )
    {
        return new WorkerCommand<Object,Boolean>()
        {
            @Override
            public Boolean doWork( Object state ) throws Exception
            {
                boolean constraintCreationFailed = false;

                try ( Transaction tx = db.beginTx() )
                {
                    db.execute( query );
                    tx.success();
                }
                catch ( QueryExecutionException e )
                {
                    System.out.println( "Constraint failed: " + e.getMessage() );
                    if ( Exceptions.rootCause( e ) instanceof ConstraintVerificationFailedKernelException )
                    {
                        // Unable to create constraint since it is not consistent with existing data
                        constraintCreationFailed = true;
                    }
                    else
                    {
                        throw e;
                    }
                }

                if ( !constraintCreationFailed )
                {
                    System.out.println( "Constraint created: " + query );
                }

                return constraintCreationFailed;
            }
        };
    }
}
