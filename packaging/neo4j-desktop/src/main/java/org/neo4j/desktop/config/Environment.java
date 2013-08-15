package org.neo4j.desktop.config;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static java.awt.Desktop.getDesktop;
import static java.awt.Desktop.isDesktopSupported;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

import static org.neo4j.desktop.ui.Components.alert;

public class Environment
{
    private final File appFile;

    public Environment() throws URISyntaxException
    {
        appFile = new File( Environment.class.getProtectionDomain().getCodeSource().getLocation().toURI() );
    }

    public File getExtensionsDirectory()
    {
        return new File( getBaseDirectory(), "extensions" );
    }

    public File getBaseDirectory()
    {
        return appFile != null ? appFile.getParentFile() : new File( "." ).getAbsoluteFile().getParentFile();
    }

    public void openBrowser( String link )
    {
        if ( desktopSupports( Desktop.Action.BROWSE ) )
        {
            try
            {
                getDesktop().browse( new URI( link ) );
            }
            catch ( IOException | URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public void editFile( File file ) throws IOException
    {
        if ( desktopSupports( Desktop.Action.EDIT ) )
        {
            try
            {
                getDesktop().edit( file );
                return;
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }

        if ( OperatingSystemFamily.WINDOWS.isDetected() )
        {
            getRuntime().exec( new String[] { "rundll32", "url.dll,FileProtocolHandler", file.getAbsolutePath() } );
            return;
        }

        alert( format( "Could not edit file %s", file.getAbsoluteFile() ) ) ;
    }

    private boolean desktopSupports( Desktop.Action action )
    {
        return isDesktopSupported() && getDesktop().isSupported( action );
    }
}
