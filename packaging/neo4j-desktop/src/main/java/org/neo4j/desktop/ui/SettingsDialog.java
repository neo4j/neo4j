/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListModel;

import org.neo4j.desktop.config.Environment;

import static java.lang.String.format;
import static javax.swing.BoxLayout.Y_AXIS;

import static org.neo4j.desktop.ui.Components.createHorizontalSpacing;
import static org.neo4j.desktop.ui.Components.createPanel;
import static org.neo4j.desktop.ui.Components.createSpacingBorder;
import static org.neo4j.desktop.ui.Components.createTextButton;
import static org.neo4j.desktop.ui.Components.createUnmodifiableTextField;
import static org.neo4j.desktop.ui.Components.createVerticalSpacing;
import static org.neo4j.desktop.ui.Components.ellipsis;
import static org.neo4j.desktop.ui.Components.withBorder;
import static org.neo4j.desktop.ui.Components.withBoxLayout;
import static org.neo4j.desktop.ui.Components.withFlowLayout;
import static org.neo4j.desktop.ui.Components.withLayout;
import static org.neo4j.desktop.ui.Components.withSpacingBorder;
import static org.neo4j.desktop.ui.Components.withTitledBorder;

class SettingsDialog extends JDialog
{
    private final Environment environment;
    private final DesktopModel model;

    SettingsDialog( Frame owner, Environment environment, DesktopModel model )
    {
        super( owner, "Neo4j Settings", true );
        this.environment = environment;
        this.model = model;

       getContentPane().add( withSpacingBorder( withBoxLayout( Y_AXIS, createPanel(
            createEditConfigPanel( createEditDatabaseConfigurationButton() ),
            createEditVmOptionsPanel( createEditVmOptionsButton() ),
            createExtensionsPanel(),
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
        setResizable( false );
    }

    private void close()
    {
        setVisible( false );
    }

    private Component createEditConfigPanel( JButton configurationButton )
    {
        String configFilePath = model.getDatabaseConfigurationFile().getAbsolutePath();
        return withFlowLayout( withTitledBorder( "Database Configuration",
            createPanel( createUnmodifiableTextField( configFilePath ), configurationButton ) ) );
    }

    private Component createEditVmOptionsPanel( JButton editVmOptionsButton )
    {
        File vmOptionsFile = model.getVmOptionsFile();
        String vmOptionsPath = vmOptionsFile == null ? "" : vmOptionsFile.getAbsolutePath();

        return withFlowLayout( withTitledBorder( "Java VM Options",
                createPanel( createUnmodifiableTextField( vmOptionsPath ), editVmOptionsButton ) ) );
    }

    private JPanel createExtensionsPanel()
    {
        DefaultListModel<String> packageListModel = createPackageListModel();
        JList<String> packageList = new JList<>( packageListModel );
        Component listPane = new JScrollPane( packageList );
        Component buttonsPane = createPackageListButtons( packageListModel, packageList );
        return withBorder(
            BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder(
                    format( "Server Extensions (at %s)", environment.getExtensionsDirectory().getAbsolutePath() ) ),
                    createSpacingBorder( 1 ) ),
            createContentPanel( listPane, buttonsPane ) );
    }

    private DefaultListModel<String> createPackageListModel()
    {
        final DefaultListModel<String> packageListModel = new DefaultListModel<>();
        for ( String packageName : model.getExtensionPackagesConfig() )
        {
            packageListModel.addElement( packageName );
        }
        return packageListModel;
    }

    private JPanel createContentPanel( Component listPanel, Component buttonsPanel )
    {
        JPanel content = withLayout( new GridBagLayout(), createPanel() );
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        content.add( createHorizontalSpacing(), c );
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 100;
        content.add( listPanel, c );
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        content.add( createHorizontalSpacing(), c );
        content.add( buttonsPanel, c );
        return content;
    }

    private JPanel createPackageListButtons( DefaultListModel<String> packageListModel, JList<String> packageList )
    {
        JButton addButton = createAddButton( packageListModel );
        JButton removeButton = createRemoveButton( packageListModel, packageList );

        JPanel packageListButtons = withLayout( new GridBagLayout(), createPanel() );
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTH;
        c.gridx = 0;
        c.gridy = 0;
        packageListButtons.add( addButton, c );
        c.gridy = 1;
        packageListButtons.add( removeButton, c );
        c.gridy = 2;
        packageListButtons.add( Box.createVerticalGlue() );
        packageListButtons.setMinimumSize( removeButton.getPreferredSize() );
        return packageListButtons;
    }

    private JButton createRemoveButton( final DefaultListModel<String> packageListModel,
                                        final JList<String> packageList )
    {
        return createTextButton( "Remove", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                int selectedIndex = packageList.getSelectedIndex();
                if ( selectedIndex != -1 )
                {
                    packageListModel.removeElementAt( selectedIndex );
                    model.setExtensionPackagesConfig( itemsAsList( packageListModel ) );
                }
            }
        } );
    }

    private JButton createAddButton( final DefaultListModel<String> packageListModel )
    {
        return createTextButton( "Add", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                String newPackage = JOptionPane.showInputDialog( "Package containing extension(s) to include" );
                if ( newPackage != null )
                {
                    packageListModel.addElement( newPackage );
                    model.setExtensionPackagesConfig( itemsAsList( packageListModel ) );
                }
            }
        } );
    }


    private List<String> itemsAsList( ListModel<String> model )
    {
        List<String> list = new ArrayList<>( model.getSize() );
        for ( int i = 0; i < model.getSize(); i++ )
        {
            list.add( model.getElementAt( i ) );
        }
        return list;
    }

    private JButton createEditDatabaseConfigurationButton()
    {
        return Components.createTextButton( ellipsis( "Edit" ), new EditFileActionListener( this, environment )
        {
            @Override
            protected File getFile()
            {
                return model.getDatabaseConfigurationFile();
            }
        } );
    }

    private JButton createEditVmOptionsButton()
    {
        return Components.createTextButton( ellipsis( "Edit" ), new EditFileActionListener( this, environment )
        {
            @Override
            protected File getFile()
            {
                return model.getVmOptionsFile();
            }
        } );
    }
}
