package org.neo4j.release.it.std.exec;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Explorative tests for using pax-url to get artifacts.
 */
public class PaxUrlTest {

    @Test
    public void use()
        throws IOException
    {
        System.setProperty( "java.protocol.handler.pkgs", "org.ops4j.pax.url" );
        new URL( "mvn:group/artifact/0.1.0" );
    }

    @Test
    public void download()
        throws IOException
    {
        System.setProperty( "java.protocol.handler.pkgs", "org.ops4j.pax.url" );
        new URL( "mvn:org.neo4j/neo4j-kernel/1.1" ).openStream();
    }
    
}
