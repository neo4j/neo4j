package org.neo4j.ha;

import org.junit.Ignore;
import org.neo4j.kernel.EmbeddedGraphDatabase;

@Ignore
public class CreateEmptyDb
{
    public static void main( String[] args )
    {
        new EmbeddedGraphDatabase( args[0] ).shutdown();
    }
}
