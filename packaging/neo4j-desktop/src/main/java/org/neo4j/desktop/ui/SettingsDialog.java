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
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;

import static org.neo4j.desktop.ui.Components.alert;
import static org.neo4j.desktop.ui.Components.createLabel;
import static org.neo4j.desktop.ui.Components.createPanel;
import static org.neo4j.desktop.ui.Components.createTextButton;
import static org.neo4j.desktop.ui.Components.createUnmodifiableTextField;
import static org.neo4j.desktop.ui.Components.createVerticalSpacing;
import static org.neo4j.desktop.ui.Components.ellipsis;
import static org.neo4j.desktop.ui.Components.withBoxLayout;
import static org.neo4j.desktop.ui.Components.withFlowLayout;
import static org.neo4j.desktop.ui.Components.withSpacingBorder;
import static org.neo4j.desktop.ui.Components.withTitledBorder;

class SettingsDialog extends JDialog
{
    private final DesktopModel model;

    SettingsDialog( Frame owner, DesktopModel model )
    {
        super( owner, "Neo4j Community - Options", true );
        this.model = model;

        getContentPane().add( withSpacingBorder( withBoxLayout( BoxLayout.Y_AXIS, createPanel(
                createCommandPromptPanel( createCommandPromptButton() ),
                createEditDatabaseConfigPanel( createEditDatabaseConfigurationButton() ),
                createEditServerConfigPanel( createEditServerConfigurationButton() ),
                createEditVmOptionsPanel( createEditVmOptionsButton() ),
                createExtensionsPanel( createOpenPluginsDirectoryButton() ),
                createVerticalSpacing(),
                withFlowLayout( FlowLayout.RIGHT, createPanel(
                        createTextButton( "Close", new ActionListener()
                        {
                            @Override
                            public void actionPerformed( ActionEvent e )
                            {
                                close();
                            }
                        } ) ) )
        ) ) ) );

        pack();
    }

    private void close()
    {
        setVisible( false );
    }

    private Component createCommandPromptPanel( JButton commandPromptButton )
    {
        return withTitledBorder( "Command-line Tools", withBoxLayout( BoxLayout.Y_AXIS,
                createPanel(
                        withFlowLayout( FlowLayout.LEFT, createPanel( createLabel(
                                "Use the command prompt to run command-line tools such as neo4j-shell and neo4j-import."
                        ) ) ),
                        withFlowLayout( FlowLayout.RIGHT, createPanel( commandPromptButton ) ) ) ) );
    }

    private Component createEditDatabaseConfigPanel( JButton configurationButton )
    {
        File configFile = model.getDatabaseConfigurationFile();
        return withTitledBorder( "Database Tuning", withBoxLayout( BoxLayout.Y_AXIS,
                createPanel(
                        withFlowLayout( FlowLayout.LEFT, createPanel( createLabel(
                                "neo4j.properties contains tuning configuration such as cache settings.",
                                "You will need to stop and re-start the database for changes to take effect."
                        ) ) ),
                        withFlowLayout( FlowLayout.RIGHT, createPanel(
                                createUnmodifiableTextField( configFile.getAbsolutePath() ),
                                configurationButton
                        ) )
                )
        ) );
    }

    private Component createEditServerConfigPanel( JButton configurationButton )
    {
        File configFile = model.getServerConfigurationFile();

        return withTitledBorder( "Server Configuration", withBoxLayout( BoxLayout.Y_AXIS,
                createPanel(
                        withFlowLayout( FlowLayout.LEFT, createPanel( createLabel(
                                configFile.getName() + " contains server configuration such as port bindings.",
                                "You will need to stop and re-start the database for changes to take effect."
                        ) ) ),
                        withFlowLayout( FlowLayout.RIGHT, createPanel(
                                createUnmodifiableTextField( configFile.getAbsolutePath() ),
                                configurationButton
                        ) )
                )
        ) );
    }

    private Component createEditVmOptionsPanel( JButton editVmOptionsButton )
    {
        File vmOptionsFile = model.getVmOptionsFile();
        return withTitledBorder( "Java VM Tuning", withBoxLayout( BoxLayout.Y_AXIS,
                createPanel(
                        withFlowLayout( FlowLayout.LEFT, createPanel( createLabel(
                                vmOptionsFile.getName() + " is for adjusting Java VM settings, such as memory usage.",
                                "You will need to close and re-start this application for changes to take effect."
                        ) ) ),
                        withFlowLayout( FlowLayout.RIGHT, createPanel(
                                createUnmodifiableTextField( vmOptionsFile.getAbsolutePath() ),
                                editVmOptionsButton
                        ) )
                )
        ) );
    }

    private Component createExtensionsPanel( JButton openPluginsDirectoryButton )
    {
        String pluginsDirectory = model.getPluginsDirectory().getAbsolutePath();
        return withTitledBorder( "Plugins and Extensions", withBoxLayout( BoxLayout.Y_AXIS,
                createPanel(
                        withFlowLayout( FlowLayout.LEFT, createPanel( createLabel(
                                "Neo4j looks for Server Plugins and Unmanaged Extensions in this folder."
                        ) ) ),
                        withFlowLayout( FlowLayout.RIGHT, createPanel(
                                createUnmodifiableTextField( pluginsDirectory ),
                                openPluginsDirectoryButton
                        ) )
                )
        ) );
    }

    private JButton createCommandPromptButton()
    {
        return Components.createTextButton( ellipsis( "Command Prompt" ), new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                try
                {
                    model.launchCommandPrompt();
                }
                catch ( Exception exception )
                {
                    exception.printStackTrace();
                    alert( exception.getMessage() );
                }
            }
        } );
    }

    private JButton createEditDatabaseConfigurationButton()
    {
        return Components.createTextButton( ellipsis( "Edit" ), new EditFileActionListener( this, model )
        {
            @Override
            protected File getFile()
            {
                return model.getDatabaseConfigurationFile();
            }

            @SuppressWarnings("ResultOfMethodCallIgnored")
            @Override
            protected void ensureFileAndParentDirectoriesExists( File file ) throws IOException
            {
                file.getParentFile().mkdirs();
                if ( !file.exists() )
                {
                    model.writeDefaultDatabaseConfiguration( file );
                }
            }
        } );
    }

    private JButton createEditServerConfigurationButton()
    {
        return Components.createTextButton( ellipsis( "Edit" ), new EditFileActionListener( this, model )
        {
            @Override
            protected File getFile()
            {
                return model.getServerConfigurationFile();
            }

            @SuppressWarnings("ResultOfMethodCallIgnored")
            @Override
            protected void ensureFileAndParentDirectoriesExists( File file ) throws IOException
            {
                file.getParentFile().mkdirs();
                if ( !file.exists() )
                {
                    model.writeDefaultServerConfiguration( file );
                }
            }
        } );
    }

    private JButton createEditVmOptionsButton()
    {
        return Components.createTextButton( ellipsis( "Edit" ), new EditFileActionListener( this, model )
        {
            @Override
            protected File getFile()
            {
                return model.getVmOptionsFile();
            }
        } );
    }

    private JButton createOpenPluginsDirectoryButton()
    {
        return Components.createTextButton( "Open",
                new OpenDirectoryActionListener( this, model.getPluginsDirectory(), model ) );
    }
}
