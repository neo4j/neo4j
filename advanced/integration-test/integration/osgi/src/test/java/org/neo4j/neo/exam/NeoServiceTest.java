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
package org.neo4j.neo.exam;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.ops4j.pax.exam.Inject;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.osgi.framework.BundleContext;

import static org.ops4j.pax.exam.CoreOptions.*;

import org.ops4j.pax.exam.Customizer;
import org.ops4j.pax.exam.Inject;
import org.ops4j.pax.exam.Option;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.*;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;

/**
 * A pax-exam integration test to verify that a NeoService can be instantiated
 * and used in an OSGi environment.
 * 
 */
@RunWith( JUnit4TestRunner.class )
public class NeoServiceTest
{

    private static final String NEO_VERSION = "1.2-SNAPSHOT";

    @Configuration
    public static Option[] config()
    {
        return options(

                logProfile(),
                cleanCaches(),
                rawPaxRunnerOption("log","debug"),
                mavenBundle().artifactId( "neo4j-kernel" ).groupId("org.neo4j" ).version("1.1"),
                mavenBundle().artifactId( "geronimo-jta_1.1_spec" )
                  .groupId( "org.apache.geronimo.specs" ).versionAsInProject(),
                repository("http://m2.neo4j.org")
                  
            );
    }

    public enum MyRelationshipTypes implements RelationshipType
    {
        KNOWS
    }

    @Inject
    private BundleContext bundleContext;

    public GraphDatabaseService neo;

    @Before
    public void setupNeo()
    {
        neo = new EmbeddedGraphDatabase( "var/neo" );
    }

    @After
    public void shutdownNeo()
    {
        neo.shutdown();
    }

    /**
     * A sanity check to make sure that a valid OSGi context exists.
     */
    @Test
    public void shouldHaveABundleContext()
    {
        assertThat( bundleContext, is( notNullValue() ) );
    }

    /**
     * A simple test to exercise the NeoService.
     */
    @Test
    public void shouldCreateSmallNodeSpace()
    {
        final String PROPERTY_NAME = "message";
        final String FIRST_NODE_VALUE = "Hello, ";
        final String SECOND_NODE_VALUE = "world!";
        final String RELATIONSHIP_VALUE = "brave Neo ";

        Transaction tx = neo.beginTx();
        try
        {
            tx.success();

            Node firstNode = neo.createNode();
            Node secondNode = neo.createNode();
            Relationship relationship = firstNode.createRelationshipTo(
                    secondNode, MyRelationshipTypes.KNOWS );

            firstNode.setProperty( PROPERTY_NAME, FIRST_NODE_VALUE );
            secondNode.setProperty( PROPERTY_NAME, SECOND_NODE_VALUE );
            relationship.setProperty( PROPERTY_NAME, RELATIONSHIP_VALUE );

            assertThat( (String) firstNode.getProperty( PROPERTY_NAME ),
                    is( equalTo( FIRST_NODE_VALUE ) ) );
            assertThat( (String) secondNode.getProperty( PROPERTY_NAME ),
                    is( equalTo( SECOND_NODE_VALUE ) ) );
            assertThat( (String) relationship.getProperty( PROPERTY_NAME ),
                    is( equalTo( RELATIONSHIP_VALUE ) ) );

        }
        finally
        {
            tx.finish();
        }

    }

}
