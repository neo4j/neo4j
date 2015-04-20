/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.ui;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import static java.lang.String.format;
import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.showMessageDialog;
import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedMessageDialog;

public abstract class EditFileActionListener implements ActionListener
{
    private final Component parentComponent;
    private final DesktopModel model;

    EditFileActionListener( Component parentComponent, DesktopModel model )
    {
        this.parentComponent = parentComponent;
        this.model = model;
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
            model.editFile( file );
        }
        catch ( IOException e )
        {
            e.printStackTrace( System.out );
            showWrappedMessageDialog( parentComponent,
                    format( "Couldn't open %s, please open the file manually",
                            file.getAbsolutePath() ),
                    "Error",
                    ERROR_MESSAGE );
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void ensureFileAndParentDirectoriesExists( File file ) throws IOException
    {
        file.getParentFile().mkdirs();
        if ( !file.exists() )
        {
            file.createNewFile();
        }
    }
}
