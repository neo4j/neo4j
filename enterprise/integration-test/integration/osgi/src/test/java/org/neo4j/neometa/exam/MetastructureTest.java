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
package org.neo4j.neometa.exam;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;
import static org.ops4j.pax.exam.CoreOptions.mavenConfiguration;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.provision;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.cleanCaches;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.logProfile;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.rawPaxRunnerOption;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.repository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.meta.model.MetaModel;
import org.neo4j.meta.model.MetaModelClass;
import org.neo4j.meta.model.MetaModelImpl;
import org.neo4j.meta.model.MetaModelNamespace;
import org.neo4j.meta.model.MetaModelProperty;
import org.ops4j.pax.exam.Inject;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.osgi.framework.BundleContext;

/**
 * A pax-exam integration test for using the neo-meta bundle in an OSGi
 * environment.
 * 
 */
@RunWith( JUnit4TestRunner.class )
public class MetastructureTest
{

    @Inject
    private BundleContext bundleContext;

    @Configuration
    public static Option[] config()
    {
        return options(
                logProfile(),
                cleanCaches(),
                rawPaxRunnerOption( "log", "debug" ),
                mavenBundle().artifactId( "geronimo-jta_1.1_spec" ).groupId(
                "org.apache.geronimo.specs" ).version( "1.1.1" ),
                mavenBundle().artifactId( "neo4j-kernel" ).groupId( "org.neo4j" ).versionAsInProject(),
                mavenBundle().artifactId( "neo4j-index" ).groupId( "org.neo4j" ).versionAsInProject(),
                mavenBundle().artifactId( "neo4j-shell" ).groupId( "org.neo4j" ).versionAsInProject(),
                mavenBundle().artifactId( "neo4j-utils" ).groupId( "org.neo4j" ).versionAsInProject(),
                mavenBundle().artifactId( "org.apache.servicemix.bundles.lucene" )
                  .groupId( "org.apache.servicemix.bundles").versionAsInProject(),
                mavenBundle().artifactId( "neo4j-meta-model" ).groupId(
                        "org.neo4j" ).versionAsInProject(),
                wrappedBundle(mavenBundle().artifactId( "jline" ).groupId( "jline" )
                  .versionAsInProject()),
                repository("http://m2.neo4j.org")
               );
    }

    public GraphDatabaseService neo4j;

    @Before
    public void setupNeo()
    {
        neo4j = new EmbeddedGraphDatabase( "target/var/neo4j" );
    }

    @After
    public void shutdownNeo()
    {
        neo4j.shutdown();
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
     * A simple test to exercise the meta-model api.
     */
    @Test
    public void shouldCreateMetaModel()
    {
        IndexService index = new LuceneIndexService( neo4j );
        MetaModel meta = new MetaModelImpl( neo4j, index );
        Transaction tx = neo4j.beginTx();
        try
        {
            MetaModelNamespace namespace = meta.getGlobalNamespace();

            // Create a class, use ", true" for "create it if it doesn't exist".
            MetaModelClass personClass = namespace.getMetaClass(
                    "http://metaexample.org/meta#Person", true );

            // Create a property in a similar way.
            MetaModelProperty nameProperty = namespace.getMetaProperty(
                    "http://metaexample.org/meta#name", true );

            // Tell the meta model that persons can have name properties.
            personClass.getDirectProperties().add( nameProperty );
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
    }

}
