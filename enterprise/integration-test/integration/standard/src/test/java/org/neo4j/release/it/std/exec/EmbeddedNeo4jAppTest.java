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

import org.junit.Test;
import org.ops4j.pax.runner.platform.PlatformException;

import static org.neo4j.release.it.std.exec.FluentPlatformBuilder.*;
import static org.neo4j.release.it.std.exec.FluentPlatformBuilder.includingTargetTestClasses;
import static org.neo4j.release.it.std.exec.FluentPlatformBuilder.provisioning;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;

/**
 * Spawning integration test for EmbeddedNeo4jApp.
 */
public class EmbeddedNeo4jAppTest
{
    @Test
    public void shouldShutdownCleanly() throws PlatformException
    {
        final String mainClassName = EmbeddedNeo4jApp.class.getName();
        ObservablePlatform platform = buildPlatform(
                forMainClass(named(mainClassName)),
                includingTargetClasses(),
                includingTargetTestClasses(),
                provisioning(
                        mavenBundle().groupId( "org.apache.geronimo.specs" ).artifactId( "geronimo-jta_1.1_spec" ).version( "1.1.1"),
                        mavenBundle().groupId("org.neo4j").artifactId("neo4j-kernel").version("1.2-1.2.M01")
                )
        );
        platform.start();
    }
}
