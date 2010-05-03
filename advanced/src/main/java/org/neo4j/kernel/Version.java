package org.neo4j.kernel;

final class Version
{
    private Version()
    {
    }

    static String get()
    {
        // TODO: get base version info from the pom (or similar)
        StringBuilder version = new StringBuilder( "neo4j-kernel" );
        if ( true )
        {
            version.append( "-SNAPSHOT" );
        }
        if ( REVISION != null || DATE != null )
        {
            version.append( " (" );
            if ( REVISION != null )
            {
                version.append( "revision " );
                version.append( REVISION );
            }
            if ( DATE != null )
            {
                if ( REVISION != null )
                {
                    version.append( ", " );
                }
                version.append( DATE );
            }
            version.append( ")" );
        }
        return version.toString();
    }

    private static final String SVN_REVISION = "$Revision$";
    private static final String SVN_DATE = "$Date$";

    private static final String REVISION = trimSvn( SVN_REVISION );
    private static final String DATE = trimSvn( SVN_DATE );

    private static String trimSvn( String svnProperty )
    {
        int index = svnProperty.indexOf( ':' );
        if ( index > 0 )
        {
            return svnProperty.substring( index, svnProperty.length() - 1 ).trim();
        }
        return null;
    }
}
