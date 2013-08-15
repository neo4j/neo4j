package org.neo4j.desktop.config;

public enum OperatingSystemFamily
{
    WINDOWS,
    MAC_OS,
    UNIX;

    public boolean isDetected()
    {
        return this == detect();
    }

    public static OperatingSystemFamily detect()
    {
        String osName = System.getProperty( "os.name" );

        // Works according to: http://www.osgi.org/Specifications/Reference
        if ( osName.startsWith( "Windows" ) )
        {
            return WINDOWS;
        }

        if ( osName.startsWith( "Mac OS" ) )
        {
            return MAC_OS;
        }

        return UNIX;
    }
}
