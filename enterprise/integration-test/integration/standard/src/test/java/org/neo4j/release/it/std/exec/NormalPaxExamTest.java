package org.neo4j.release.it.std.exec;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;

import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.*;
import static org.ops4j.pax.exam.container.def.PaxRunnerOptions.vmOption;

/**
 */
@RunWith(JUnit4TestRunner.class)
public class NormalPaxExamTest {

    @Configuration
    public Option[] configure() {
        return options(
                systemProperty("hello.name").value("world"),
                provision(
                    mavenBundle().groupId("org.ops4j.pax.url").artifactId("pax-url-mvn").version("1.1.2")
                ),
                vmOption("-d64 -server -Xms1G -Xmx1G -verbose:gc"),
                felix()
        );
    }

    @Test
    public void shouldForkFelix() {
        assertTrue(true);
    }
}
