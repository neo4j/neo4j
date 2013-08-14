package org.neo4j.desktop.ui;

import java.io.File;

import static java.lang.String.format;

class UnsuitableGraphDatabaseDirectory extends Exception
{
    UnsuitableGraphDatabaseDirectory( String message, File dir )
    {
        super( format( message, dir.getAbsolutePath() ) );
    }
}
