package org.neo4j.kernel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

enum Version
{
    VERSION;
    private final String title;
    private final String vendor;
    private final String version;
    private final String revision;

    @Override
    public String toString()
    {
        if ( title == null )
        {
            return "Neo4j Kernel, unpackaged version " + getVersion();
        }
        return title + " " + getVersion();
    }

    private String getVersion()
    {
        if ( version == null )
        {
            return "<unknown>";
        }
        else if ( version.contains( "SNAPSHOT" ) )
        {
            return version + " (revision: " + revision + ")";
        }
        else
        {
            return version;
        }
    }

    private Version()
    {
        Package pkg = getClass().getPackage();
        this.title = pkg.getImplementationTitle();
        this.vendor = pkg.getImplementationVendor();
        this.version = pkg.getImplementationVersion();
        @SuppressWarnings( "hiding" ) String revision = null;
        Manifest manifest = getManifest();
        if ( manifest != null )
        {
            Attributes attr = manifest.getAttributes( pkg.getName().replace(
                    '.', '/' ) );
            if ( attr == null )
            {
                attr = manifest.getMainAttributes();
            }
            if ( attr != null )
            {
                revision = revision(
                        attr.getValue( "Implementation-Revision" ),
                        attr.getValue( "Implementation-Revision-Status" ) );
            }
        }
        this.revision = revision;
    }

    private static String revision( String revision, String status )
    {
        if ( revision != null )
        {
            StringBuilder result = new StringBuilder( revision );
            if ( status != null )
            {
                status = mapOf( status ).get( "status" );
                if ( status != null )
                {
                    result.append( status );
                }
            }
            return result.toString();
        }
        return "";
    }

    private static Map<String, String> mapOf( String string )
    {
        Map<String, String> result = new HashMap<String, String>();
        for ( String part : string.split( ";" ) )
        {
            int start = part.indexOf( '=' );
            if ( start == -1 )
            {
                result.put( part, "true" );
            }
            else
            {
                result.put( part.substring( 0, start ),
                        part.substring( start + 1 ) );
            }
        }
        return result;
    }

    private Manifest getManifest()
    {
        try
        {
            return new JarFile( new File( getClass().getProtectionDomain()
                    .getCodeSource().getLocation().toURI() ) ).getManifest();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    public static void main( String[] args )
    {
        System.out.println( VERSION );
    }

    static String get()
    {
        return VERSION.toString();
    }
}
