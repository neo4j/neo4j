/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.osgi;

import static org.ops4j.pax.exam.CoreOptions.cleanCaches;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;
import static org.ops4j.pax.exam.CoreOptions.frameworkStartLevel;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.provision;
import static org.ops4j.pax.exam.CoreOptions.repository;
import static org.ops4j.pax.tinybundles.core.TinyBundles.bundle;
import static org.ops4j.pax.tinybundles.core.TinyBundles.withBnd;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.Index;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.player.Player;
import org.ops4j.pax.exam.spi.PaxExamRuntime;
import org.ops4j.pax.exam.testforge.BundlesInState;
import org.ops4j.pax.exam.testforge.CountBundles;
import org.ops4j.pax.exam.testforge.WaitForService;
import org.osgi.framework.Bundle;
import org.osgi.framework.Constants;

public class OSGiTest
{

    public static final String NEO4J_VERSION = "1.8-SNAPSHOT";
    public static final String GERONIMO_JTA_VERSION = "1.1.1";

    @Test
    public void neo4jStartupTestFelix() throws Exception
    {

        Option[] options = testOptions();
        Player player = new Player().with( options );
        test( player, 19 );
    }

    public Option[] gogoShellOptions()
    {
        String gogoVersion = "0.8.0";
        return options(
                mavenBundle().groupId( "org.ops4j.pax.logging" ).artifactId(
                        "pax-logging-api" ).version( "1.6.1" ),
                mavenBundle().groupId( "org.osgi" ).artifactId(
                        "org.osgi.compendium" ).version( "4.2.0" ),
                mavenBundle().groupId( "org.apache.felix" ).artifactId(
                        "org.apache.felix.gogo.runtime" ).version( gogoVersion ),
                mavenBundle().groupId( "org.apache.felix" ).artifactId(
                        "org.apache.felix.gogo.shell" ).version( gogoVersion ),
                mavenBundle().groupId( "org.apache.felix" ).artifactId(
                        "org.apache.felix.gogo.command" ).version( gogoVersion ) );
    }

    public Option[] testOptions()
    {
        Option[] options = options(
                repository( "https://oss.sonatype.org/content/groups/ops4j/" ),
                cleanCaches(),
                frameworkStartLevel( 10 ),
                mavenBundle().groupId( "org.apache.geronimo.specs" ).artifactId(
                        "geronimo-jta_1.1_spec" ).version( GERONIMO_JTA_VERSION ),
                wrappedBundle( mavenBundle().groupId( "org.apache.lucene" ).artifactId(
                        "lucene-core" ).version( "3.5.0" ) ),
                wrappedBundle( mavenBundle().groupId( "org.neo4j" ).artifactId(
                        "neo4j-kernel" ).version( NEO4J_VERSION ) ),
                wrappedBundle( mavenBundle().groupId( "org.neo4j" ).artifactId(
                        "neo4j-lucene-index" ).version( NEO4J_VERSION ) ),
                provision( bundle().add( Neo4jActivator.class ).set(
                        Constants.BUNDLE_ACTIVATOR,
                        Neo4jActivator.class.getName() ).build( withBnd() ) ) );
        return options;
    }

    private void test( Player player, int expectedBundles ) throws Exception
    {
        player.test( WaitForService.class,
                GraphDatabaseService.class.getName(), 10000 ).test(
                WaitForService.class, Index.class.getName(), 15000 ).test(
                CountBundles.class, expectedBundles ).test(
                BundlesInState.class, Bundle.ACTIVE, Bundle.ACTIVE ).play();
    }

    public static void main( String[] args ) throws Exception
    {
        // create a proper ExamSystem with your options. Focus on
        // "createServerSystem"
        OSGiTest instance = new OSGiTest();
        ArrayList<Option> ops = new ArrayList<Option>();
        ops.addAll( Arrays.asList( instance.testOptions() ) );
        ops.addAll( Arrays.asList( instance.gogoShellOptions() ) );
        ExamSystem system = PaxExamRuntime.createServerSystem( (Option[]) ops.toArray( new Option[] {} ) );
        // create Container (you should have exactly one configured!) and start.
        TestContainer container = PaxExamRuntime.createContainer( system );
        container.start();

    }
}
