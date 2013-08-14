package org.neo4j.desktop.ui;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import org.neo4j.desktop.config.Value;

public class DesktopModel
{
    private File databaseDirectory;
    private final Value<List<String>> extensionPackagesConfig;

    public DesktopModel( Value<List<String>> extensionPackagesConfig )
    {
        this.extensionPackagesConfig = extensionPackagesConfig;
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public void setDatabaseDirectory( File databaseDirectory )
    {
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

    public String[] getExtensionPackagesConfigAsArray()
    {
        List<String> list = getExtensionPackagesConfig();
        return list.toArray( new String[list.size()] );
    }

    public void setExtensionPackagesConfig( List<String> value )
    {
        extensionPackagesConfig.set( value );
    }

    public File getDatabaseConfigurationFile()
    {
        return new File( databaseDirectory, "neo4j.properties" );
    }
}
