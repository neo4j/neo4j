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
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.util.ArrayList;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
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
import static javax.swing.BoxLayout.Y_AXIS;
import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.showConfirmDialog;
import static javax.swing.SwingUtilities.invokeLater;
import static javax.swing.filechooser.FileSystemView.getFileSystemView;

import static org.neo4j.desktop.ui.SwingHelper.loadImage;

/**
 * The main window of the Neo4j Desktop. Able to start/stop a database as well as providing access to some
 * advanced configuration options, such as heap size and database properties.
 */
public class MainWindow
{
    private final DesktopModel model;

    public static enum DatabaseStatus
    {
        stopped,
        starting,
        started,
        stopping
    }
    
    private final JFrame frame;
    private final DatabaseActions databaseActions;
    private JButton selectButton;
    private JButton settingsButton;
    private JButton startButton;
    private JButton stopButton;
    private CardLayout statusPanelLayout;
    private JPanel statusPanel;
    private JTextField directoryDisplay;
    private SystemOutDebugWindow debugWindow;
    private DatabaseStatus databaseStatus; // Not used a.t.m. but may be used for something?
    private final Environment environment;
    private final SysTray sysTray;

    public MainWindow( final DatabaseActions databaseActions, Environment environment, DesktopModel model )
    {
        this.model = model;
        // This is here only for debugging, comment out the line below to see system out
//        debugWindow = new SystemOutDebugWindow();
        model.setPrintStackTraces( debugWindow != null );

        this.environment = environment;
        this.databaseActions = databaseActions;

        frame = init();
        
        this.sysTray = SysTray.install( "/neo4j-systray-16.png", new SysTray.Actions()
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
                if ( databaseStatus == DatabaseStatus.stopped )
                {
                    shutdown();
                }
                else
                {
                    frame.setVisible( false );
                }
            }
        }, frame );

        goToStoppedStatus();
    }

    public void display()
    {
        frame.setLocationRelativeTo( null );
        frame.setVisible( true );
    }

    private JFrame init()
    {
        final JFrame frame = new JFrame( "Neo4j Desktop" );
        frame.setIconImages( loadIcons( "/neo4j-cherries-%d.png" ) );
        frame.add( initRootPanel() );
        frame.pack();
        frame.setResizable( false );

        return frame;
    }

    private JPanel initRootPanel()
    {
        JPanel root = new JPanel();
        root.setLayout( new BoxLayout( root, Y_AXIS ) );
        root.setBorder( BorderFactory.createEmptyBorder( 5, 5, 5, 5 ) );
        root.add( createLogoPanel() );
        root.add( initSelectionPanel() );
        root.add( statusPanel = initStatusPanel() );
        root.add ( Box.createVerticalStrut( 5 ) );
        root.add( initActionPanel() );
        return root;
    }

    private JPanel createLogoPanel()
    {
        final JPanel logoPanel = new JPanel();
        logoPanel.setLayout( new FlowLayout( FlowLayout.LEFT ) );
        logoPanel.add( new JLabel( new ImageIcon( loadImage( "/neo4j-cherries-32.png" ) ) ) );
        logoPanel.add( new JLabel( "Neo4j") );
        return logoPanel;
    }

    private JPanel initActionPanel()
    {
        final JPanel actionPanel = new JPanel();
        actionPanel.setLayout( new BoxLayout( actionPanel, BoxLayout.LINE_AXIS ) );
        actionPanel.add( settingsButton = initSettingsButton() );
        actionPanel.add( Box.createHorizontalGlue() );
        actionPanel.add( stopButton = initStopButton() );
        actionPanel.add( startButton = initStartButton() );
        return actionPanel;
    }

    private JButton initSettingsButton()
    {
        return SwingHelper.buttonWithText( SwingHelper.elipsis( "Settings" ), new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                JDialog settingsDialog = new SettingsDialog( frame, environment, model );
                settingsDialog.setVisible( true );
            }
        } );
    }

    private JPanel initSelectionPanel()
    {
        final JPanel selectionPanel = new JPanel();
        selectionPanel.setLayout( new BoxLayout( selectionPanel, BoxLayout.LINE_AXIS ) );
        selectionPanel.setBorder( BorderFactory.createTitledBorder( "Database location" ) );
        directoryDisplay = new JTextField( defaultPath(), 30 );
        directoryDisplay.setEditable( false );
        selectionPanel.add( directoryDisplay );
        selectionPanel.add( selectButton = initSelectButton( selectionPanel ) );
        return selectionPanel;
    }

    private ArrayList<Image> loadIcons( String resourcePath )
    {
        ArrayList<Image> icons = new ArrayList<>();
        for ( int i = 16; i <= 256; i *= 2 )
        {
            Image image = loadImage( format( resourcePath, i ) );
            if ( null != image )
            {
                icons.add( image );
            }
        }
        return icons;
    }

    private String defaultPath()
    {
        ArrayList<File> locations = new ArrayList<>(  );

        // Works according to: http://www.osgi.org/Specifications/Reference
        String os = System.getProperty( "os.name" );

        if ( os.startsWith( "Windows" ) )
        {

            // cf. http://stackoverflow.com/questions/1503555/how-to-find-my-documents-folder
            locations.add( getFileSystemView().getDefaultDirectory() );
        }

        if ( os.startsWith( "Mac OS" ) )
        {
            // cf. http://stackoverflow.com/questions/567874/how-do-i-find-the-users-documents-folder-with-java-in-os-x
            locations.add( new File( new File( System.getProperty( "user.home" ) ), "Documents" ) );
        }

        locations.add( new File( System.getProperty( "user.home" ) ) );

        File result = selectFirstWriteableDirectoryOrElse( locations, new File( System.getProperty( "user.dir" ) ) );
        return new File( result, "neo4j" ).getAbsolutePath();
    }

    private File selectFirstWriteableDirectoryOrElse( ArrayList<File> locations, File defaultFile )
    {
        File result = defaultFile.getAbsoluteFile();
        for ( File file : locations )
        {
            File candidateFile = file.getAbsoluteFile();
            if ( candidateFile.exists() && candidateFile.isDirectory() && candidateFile.canWrite() ) {
                result = candidateFile;
                break;
            }
        }
        return result;
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

    private JPanel initStatusPanel()
    {
        JPanel statusPanel = new JPanel();
        statusPanel.setBorder( BorderFactory.createTitledBorder( "Status" ) );
        statusPanelLayout = new CardLayout();
        statusPanel.setLayout( statusPanelLayout );
        statusPanel.add( DatabaseStatus.stopped.name(), createSimpleStatusPanel(
                new Color( 1.0f, 0.5f, 0.5f ), "Choose a graph database directory, then start the server" ) );
        statusPanel.add( DatabaseStatus.starting.name(), createSimpleStatusPanel(
                new Color( 1.0f, 1.0f, 0.5f ), SwingHelper.elipsis( "In just a few seconds, Neo4j will be ready" ) ) );
        statusPanel.add( DatabaseStatus.started.name(), createStartedStatus() );
        statusPanel.add( DatabaseStatus.stopping.name(), createSimpleStatusPanel(
                new Color( 0.7f, 0.7f, 0.7f ), SwingHelper.elipsis( "Neo4j is shutting down" ) ) );
        statusPanel.addMouseListener( new MouseAdapter()
        {
            @Override
            public void mouseClicked( MouseEvent e )
            {
                if ( e.getButton() == MouseEvent.BUTTON1 && debugWindow != null &&  e.isAltDown() )
                {
                    debugWindow.show();
                }
            }
        } );
        return statusPanel;
    }

    private JButton initSelectButton( final JPanel selectionPanel )
    {
        return SwingHelper.buttonWithText( SwingHelper.elipsis( "Browse" ), new ActionListener()
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
                    switch ( jFileChooser.showOpenDialog( selectionPanel ) )
                    {
                        default:
                            return;

                        case APPROVE_OPTION:
                            File selectedFile = jFileChooser.getSelectedFile();

                            try
                            {
                                verifyGraphDirectory( selectedFile );
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
                    }
                }
            }
        } );
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

    private JButton initStartButton()
    {
        return SwingHelper.buttonWithText( "Start", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent event )
            {
                goToStartingStatus();

                // Invoke later here to get visual feedback of the status change.
                // GUI updates happens after action performed exists.
                invokeLater( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        databaseActions.start();
                        goToStartedStatus();
                    }
                } );
            }
        } );
    }

    private JButton initStopButton()
    {
        return SwingHelper.buttonWithText( "Stop", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                goToStoppingStatus();

                // Invoke later here to get visual feedback of the status change.
                // GUI updates happens after action performed exists.
                invokeLater( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        databaseActions.stop();
                        goToStoppedStatus();
                    }
                } );
            }
        } );
    }
    
    private void displayStatus( DatabaseStatus status )
    {
        statusPanelLayout.show( statusPanel, status.name() );
        databaseStatus = status;
        sysTray.changeStatus( status );
    }

    private void goToStartingStatus()
    {
        selectButton.setEnabled( false );
        settingsButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.starting );
    }

    private void goToStartedStatus()
    {
        selectButton.setEnabled( false );
        settingsButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( true );
        displayStatus( DatabaseStatus.started );
    }

    private void goToStoppingStatus()
    {
        selectButton.setEnabled( false );
        settingsButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.stopping );
    }

    private void goToStoppedStatus()
    {
        selectButton.setEnabled( true );
        settingsButton.setEnabled( true );
        startButton.setEnabled( true );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.stopped );
    }
    
    private JPanel createSimpleStatusPanel( Color color, String text )
    {
        return createStatusPanel( color, new JLabel( text ) );
    }

    private JPanel createStartedStatus()
    {
        JLabel link = new JLabel( "http://localhost:7474/" );
        link.setFont( SwingHelper.underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link, environment ) );

        return createStatusPanel( new Color( 0.5f, 1.0f, 0.5f ), new JLabel("Neo4j is ready. Browse to "), link );
    }

    private JPanel createStatusPanel( Color color, JComponent... components )
    {
        JPanel panel = new JPanel();
        panel.setLayout( new FlowLayout() );
        panel.setBackground( color );
        for ( JComponent component : components )
        {
            panel.add( component );
        }
        return panel;
    }

    private class UnsuitableGraphDatabaseDirectory extends Exception
    {
        UnsuitableGraphDatabaseDirectory( String message, File dir )
        {
            super( format( message, dir.getAbsolutePath() ) );
        }
    }
}
