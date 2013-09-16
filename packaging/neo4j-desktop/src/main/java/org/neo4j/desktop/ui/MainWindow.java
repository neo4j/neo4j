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

import java.awt.CardLayout;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.runtime.DatabaseActions;

import static java.lang.String.format;
import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.showConfirmDialog;
import static javax.swing.SwingUtilities.invokeLater;
import static org.neo4j.desktop.ui.Components.alert;
import static org.neo4j.desktop.ui.Components.createPanel;
import static org.neo4j.desktop.ui.Components.createUnmodifiableTextField;
import static org.neo4j.desktop.ui.Components.createVerticalSpacing;
import static org.neo4j.desktop.ui.Components.ellipsis;
import static org.neo4j.desktop.ui.Components.withBoxLayout;
import static org.neo4j.desktop.ui.Components.withFlowLayout;
import static org.neo4j.desktop.ui.Components.withLayout;
import static org.neo4j.desktop.ui.Components.withSpacingBorder;
import static org.neo4j.desktop.ui.Components.withTitledBorder;
import static org.neo4j.desktop.ui.DatabaseStatus.STARTED;
import static org.neo4j.desktop.ui.DatabaseStatus.STARTING;
import static org.neo4j.desktop.ui.DatabaseStatus.STOPPED;
import static org.neo4j.desktop.ui.Graphics.loadImage;

/**
 * The main window of the Neo4j Desktop. Able to start/stop a database as well as providing access to some
 * advanced configuration options, such as heap size and database properties.
 */
public class MainWindow
{
    private final DesktopModel model;

    private final JFrame frame;
    private final DatabaseActions databaseActions;
    private final JButton browseButton;
    private final JButton settingsButton;
    private final JButton startButton;
    private final JButton stopButton;
    private final CardLayout statusPanelLayout;
    private final JPanel statusPanel;
    private final JTextField directoryDisplay;
    private final SystemOutDebugWindow debugWindow;
    private final Environment environment;
    private final SysTray sysTray;

    private DatabaseStatus databaseStatus;

    public MainWindow( final DatabaseActions databaseActions, Environment environment, DesktopModel model )
    {
        this.model = model;
        this.debugWindow = new SystemOutDebugWindow();
        this.environment = environment;
        this.databaseActions = databaseActions;

        this.frame = new JFrame( "Neo4j Community" );
        this.frame.setIconImages( Graphics.loadIcons() );
        this.sysTray = SysTray.install( new SysTrayActions(), frame );

        this.directoryDisplay = createUnmodifiableTextField( model.getDatabaseDirectory().getAbsolutePath() );
        this.browseButton = createBrowseButton();
        this.statusPanelLayout = new CardLayout();
        this.statusPanel = createStatusPanel( statusPanelLayout );
        this.startButton = createStartButton();
        this.stopButton = createStopButton();
        this.settingsButton = createSettingsButton();

        JPanel root =
                createRootPanel( directoryDisplay, browseButton, statusPanel, startButton, stopButton, settingsButton );

        frame.add( root );
        frame.pack();
        frame.setResizable( false );

        updateStatus( STOPPED );
    }

    private JPanel createRootPanel( JTextField directoryDisplay, JButton browseButton, Component statusPanel,
                                    JButton startButton, JButton stopButton, JButton settingsButton )
    {
        return withSpacingBorder( withBoxLayout( BoxLayout.Y_AXIS,
            createPanel( createLogoPanel(), createSelectionPanel( directoryDisplay, browseButton ), statusPanel,
                         createVerticalSpacing(), createActionPanel( startButton, stopButton, settingsButton ) ) ) );
    }

    public void display()
    {
        frame.setLocationRelativeTo( null );
        frame.setVisible( true );
    }

    private JPanel createLogoPanel()
    {
        return withFlowLayout( FlowLayout.LEFT, createPanel(
                new JLabel( new ImageIcon( loadImage( Graphics.LOGO_32 ) ) ),
                new JLabel( format( "Neo4j %s", model.getNeo4jVersion() ) ) ) );
    }

    private JPanel createActionPanel( JButton startButton, JButton stopButton, JButton settingsButton )
    {
        return withBoxLayout( BoxLayout.LINE_AXIS,
            createPanel( settingsButton, Box.createHorizontalGlue(), stopButton, startButton ) );
    }

    private JButton createSettingsButton()
    {
        return Components.createTextButton( ellipsis( "Settings" ), new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                JDialog settingsDialog = new SettingsDialog( frame, environment, model );
                settingsDialog.setLocationRelativeTo( null );
                settingsDialog.setVisible( true );
            }
        } );
    }

    private JPanel createSelectionPanel( JTextField directoryDisplay, JButton selectButton )
    {
        return withTitledBorder( "Database location", withBoxLayout( BoxLayout.LINE_AXIS,
            createPanel( directoryDisplay, selectButton ) ) );
    }

    protected void shutdown()
    {
        databaseActions.shutdown();
        if ( debugWindow != null )
        {
            debugWindow.dispose();
        }
        frame.dispose();
        
        // TODO Wouldn't want to have exit here really, but there's an issue where the JVM
        // is kept alive by something (possibly the "fallback" shell server in ConsoleService)
        // preventing it from shutting down properly. When that issue is fixed this should
        // preferably be removed.
        System.exit( 0 );
    }

    private JPanel createStatusPanel( CardLayout statusPanelLayout )
    {
        JPanel panel = withLayout( statusPanelLayout, withTitledBorder( "Status", createPanel() ) );
        for ( DatabaseStatus status : DatabaseStatus.values() )
        {
            panel.add( status.name(), status.display( environment ) );
        }

        panel.addMouseListener( new MouseAdapter()
        {
            @Override
            public void mouseClicked( MouseEvent e )
            {
                if ( MouseEvent.BUTTON1 == e.getButton() && e.isAltDown() )
                {
                    debugWindow.show();
                }
            }
        } );
        return panel;
    }

    private JButton createBrowseButton()
    {
        return Components.createTextButton( ellipsis( "Browse" ), new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                JFileChooser jFileChooser = new JFileChooser();
                jFileChooser.setFileSelectionMode( DIRECTORIES_ONLY );
                jFileChooser.setCurrentDirectory( new File( directoryDisplay.getText() ) );
                jFileChooser.setDialogTitle( "Select database" );
                jFileChooser.setDialogType( CUSTOM_DIALOG );

                while ( true )
                {
                    switch ( jFileChooser.showOpenDialog( frame ) )
                    {
                        case APPROVE_OPTION:
                            File selectedFile = jFileChooser.getSelectedFile();

                            try
                            {
                                model.setDatabaseDirectory( selectedFile );
                                directoryDisplay.setText( model.getDatabaseDirectory().getAbsolutePath() );
                                return;
                            }
                            catch ( UnsuitableGraphDatabaseDirectory error )
                            {
                                int result = showConfirmDialog(
                                        frame, error.getMessage() + "\nPlease choose a different folder.",
                                        "Invalid folder selected", JOptionPane.OK_CANCEL_OPTION );
                                switch ( result )
                                {
                                    case CANCEL_OPTION:
                                        return;
                                    default:
                                }
                            }

                        default:
                            return;
                }

                }
            }
        } );
    }

    private JButton createStartButton()
    {
        return Components.createTextButton( "Start", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent event )
            {
                updateStatus( STARTING );

                invokeLater( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            model.prepareGraphDirectoryForStart();

                            databaseActions.start();
                            updateStatus( STARTED );
                        }
                        catch ( UnsuitableGraphDatabaseDirectory e )
                        {
                            updateUserWithErrorMessageAndStatus( e );
                        }
                        catch ( UnableToStartServerException e )
                        {
                            updateUserWithErrorMessageAndStatus( e );
                        }
                    }

                    private void updateUserWithErrorMessageAndStatus( Exception e )
                    {
                        alert( e.getMessage() );
                        updateStatus( STOPPED );
                    }
                } );
            }
        } );
    }

    private JButton createStopButton()
    {
        return Components.createTextButton( "Stop", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                updateStatus( DatabaseStatus.STOPPING );

                invokeLater( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        databaseActions.stop();
                        updateStatus( STOPPED );
                    }
                } );
            }
        } );
    }

    private void updateStatus(DatabaseStatus status)
    {
        browseButton.setEnabled( STOPPED == status );
        settingsButton.setEnabled( STOPPED == status );
        startButton.setEnabled( STOPPED == status );
        stopButton.setEnabled( STARTED == status );
        statusPanelLayout.show( statusPanel, status.name() );
        databaseStatus = status;
        sysTray.changeStatus( status );
    }

    private class SysTrayActions implements SysTray.Actions
    {
        @Override
        public void closeForReal()
        {
            shutdown();
        }

        @Override
        public void clickSysTray()
        {
            frame.setVisible( true );
        }

        @Override
        public void clickCloseButton()
        {
            if ( databaseStatus == STOPPED )
            {
                shutdown();
            }
            else
            {
                frame.setVisible( false );
            }
        }
    }
}
