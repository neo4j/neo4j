package org.neo4j.desktop.ui;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import org.neo4j.desktop.config.Value;

public class DesktopModel
{
    private File databaseDirectory;
    private final Value<List<String>> extensionPackagesConfig;

    public DesktopModel( File databaseDirectory, Value<List<String>> extensionPackagesConfig )
    {
        this.databaseDirectory = databaseDirectory;
        this.extensionPackagesConfig = extensionPackagesConfig;
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public void setDatabaseDirectory( File databaseDirectory ) throws UnsuitableGraphDatabaseDirectory
    {
        verifyGraphDirectory( databaseDirectory );
        this.databaseDirectory = databaseDirectory;
    }

    public File getVmOptionsFile()
    {
        try
        {
            File jarFile = new File(MainWindow.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return new File( jarFile.getParentFile(), ".vmoptions" );
        }
        catch ( URISyntaxException e )
        {
            e.printStackTrace( System.out );
        }
        return null;
    }

    public List<String> getExtensionPackagesConfig()
    {
        return extensionPackagesConfig.get();
    }

    public void setExtensionPackagesConfig( List<String> value )
    {
        extensionPackagesConfig.set( value );
    }

    public File getDatabaseConfigurationFile()
    {
        return new File( databaseDirectory, "neo4j.properties" );
    }

    private void verifyGraphDirectory( File dir ) throws UnsuitableGraphDatabaseDirectory
    {
        if ( !dir.isDirectory() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not a directory", dir );
        }

        if ( !dir.canWrite() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not writeable", dir );
        }

        String[] fileNames = dir.list();
        if ( 0 == fileNames.length )
        {
            return;
        }

        for ( String fileName : fileNames )
        {
            if ( fileName.startsWith( "neostore" ) )
            {
                return;
            }
        }

        throw new UnsuitableGraphDatabaseDirectory(
                "%s is neither empty nor does it contain a neo4j graph database", dir );
    }
}
