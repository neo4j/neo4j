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
