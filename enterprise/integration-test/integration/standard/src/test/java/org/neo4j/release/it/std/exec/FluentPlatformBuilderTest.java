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
package org.neo4j.release.it.std.exec;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ops4j.pax.exam.options.ProvisionOption;
import org.ops4j.pax.runner.platform.Platform;
import org.ops4j.pax.runner.platform.PlatformException;
import org.ops4j.pax.runner.platform.ServiceConstants;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.neo4j.release.it.std.exec.FluentPlatformBuilder.*;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;

/**
 */
public class FluentPlatformBuilderTest {

    @Test
    public void shouldCreateAPlatform() {
        Platform platform = buildPlatform(forMainClass("org.neo4j.release.it.std.exec.HelloWorldApp"));

        assertThat(platform, is(notNullValue()));
    }

    @Test
    public void shouldSpecifyMainClassName() {
        final String EXPECTED_MAIN_CLASS_NAME = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(EXPECTED_MAIN_CLASS_NAME))
        );

        assertThat(platform.getMainClassName(), is(EXPECTED_MAIN_CLASS_NAME));
    }

    @Test
    public void shouldSpeciftyArgumentsForMainClass() {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        final String EXPECTED_ARG = "bob";
        ObservablePlatform platform = buildPlatform(
                forMainClass(
                        named(mainClassName),
                        usingArgs(EXPECTED_ARG)
                )
        );

        assertThat(Arrays.asList(platform.getArguments()), hasItem(EXPECTED_ARG));
    }

    @Test
    public void shouldSpecifyJvmOptions() {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        final String EXPECTED_VM_OPTION_1 = "-XXreally-fast-jvm";
        final String EXPECTED_VM_OPTION_2 = "-XXnever-fail";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                configuringJvmWith(EXPECTED_VM_OPTION_1, EXPECTED_VM_OPTION_2)
        );

        assertThat(Arrays.asList(platform.getVMOptions()), hasItem(EXPECTED_VM_OPTION_1));
        assertThat(Arrays.asList(platform.getVMOptions()), hasItem(EXPECTED_VM_OPTION_2));
    }

    @Test
    public void shouldAllowInclusionOfTargetClasses() {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetClasses()
        );

        assertThat((String) platform.getConfiguration().get(ServiceConstants.CONFIG_CLASSPATH), containsString("target" + File.separator + "classes"));
    }


    @Test
    public void shouldAllowInclusionOfTargetTestClasses() {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetTestClasses()
        );

        assertThat((String) platform.getConfiguration().get(ServiceConstants.CONFIG_CLASSPATH), containsString("target" + File.separator + "test-classes"));
    }

    @Test
    public void shouldProvideAValidPlatformDefintion() throws IOException {
        FluentPlatformBuilder builder = new FluentPlatformBuilder();
        assertThat(builder.getDefinition(null), is(notNullValue()));
    }

    @Test
    public void shouldStartPlatformUsingJustTargets() throws PlatformException {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetClasses(),
                includingTargetTestClasses()
        );
        platform.start();
    }

    @Test
    public void shouldProvisionMavenDependencies() {

        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetClasses(),
                includingTargetTestClasses(),
                provisioning(
                        mavenBundle().groupId("org.neo4j").artifactId("neo4j-kernel").version("1.2-SNAPSHOT")
                )
        );
    }

    @Test
    public void shouldStartPlatformWithMavenDependencies() throws PlatformException {
        final String mainClassName = "org.neo4j.release.it.std.exec.HelloWorldApp";
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetClasses(),
                includingTargetTestClasses(),
                provisioning(
                        mavenBundle().groupId("org.neo4j").artifactId("neo4j-kernel").version("1.2-1.2.M01")
                )
        );
        platform.start();
    }
}

