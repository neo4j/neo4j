package org.neo4j.desktop.ui;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import org.neo4j.desktop.config.Environment;

import static java.lang.String.format;
import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.showMessageDialog;

public abstract class EditFileActionListener implements ActionListener
{
    private final Component parentComponent;
    private final Environment environment;

    EditFileActionListener( Component parentComponent, Environment environment )
    {
        this.parentComponent = parentComponent;
        this.environment = environment;
    }

    protected abstract File getFile();

    @Override
    public void actionPerformed( ActionEvent event )
    {
        File file = getFile();
        if ( null == file )
        {
            showMessageDialog( parentComponent,
                "Did not find location of .vmoptions file",
                "Error",
                ERROR_MESSAGE );
            return;
        }
        try
        {
            ensureFileAndParentDirectoriesExists( file );
            environment.editFile( file );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            showMessageDialog( parentComponent,
                format("Couldn't open %s, please open the file manually", file.getAbsolutePath() ),
                "Error",
                ERROR_MESSAGE );
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void ensureFileAndParentDirectoriesExists( File file ) throws IOException
    {
        file.getParentFile().mkdirs();
        if ( !file.exists() )
        {
            file.createNewFile();
        }
    }
}
