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
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.HeapSizeConfig;
import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.runtime.DatabaseActions;

import static java.awt.font.TextAttribute.UNDERLINE;
import static java.awt.font.TextAttribute.UNDERLINE_ON;

import static javax.swing.BoxLayout.Y_AXIS;
import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.showMessageDialog;
import static javax.swing.SwingConstants.HORIZONTAL;
import static javax.swing.SwingUtilities.invokeLater;

import static org.neo4j.desktop.config.HeapSizeConfig.getAvailableTotalPhysicalMemoryMb;

public class MainWindow
{
    public static enum DatabaseStatus
    {
        stopped,
        starting,
        started,
        stopping;
    }
    
    private final JFrame frame;
    private final DatabaseActions databaseActions;
    private JButton selectButton;
    private JButton databaseConfigurationButton;
    private JButton startButton;
    private JButton stopButton;
    private CardLayout statusPanelLayout;
    private JPanel statusPanel;
    private JTextField directoryDisplay;
    private JPanel advancedPanel;
    private final Value<Integer> heapSizeConfig;
    private final SystemOutDebugWindow debugWindow;
    private DatabaseStatus databaseStatus; // Not used a.t.m. but may be used for something?

    public MainWindow( DatabaseActions databaseActions, Environment environment )
    {
        // TODO (keep the below line in for easier getting going with debugging)
        //      Only for debugging, comment out the line below for real usage
        debugWindow = new SystemOutDebugWindow();
        
        this.heapSizeConfig = new HeapSizeConfig( environment ).get();
        this.databaseActions = databaseActions;
        frame = init();
        goToStoppedStatus();
    }

    public void display()
    {
        frame.setLocationRelativeTo( null );
        frame.setVisible( true );
    }

    private JFrame init()
    {
        final JPanel selectionPanel = new JPanel();
        directoryDisplay = new JTextField( new File( "." ).getAbsoluteFile().getParentFile().getAbsolutePath(), 30 );
        directoryDisplay.setEditable( false );

        selectionPanel.add( selectButton = initSelectButton( selectionPanel ) );
        selectionPanel.add( directoryDisplay );
        selectionPanel.add( databaseConfigurationButton = initDatabaseConfigurationButton() );
        selectionPanel.add( startButton = initStartButton() );
        selectionPanel.add( stopButton = initStopButton() );
        
        JPanel root = new JPanel();
        root.setLayout( new BoxLayout( root, Y_AXIS ) );
        root.add( selectionPanel );
        root.add( statusPanel = initStatusPanel() );
        root.add( advancedPanel = initAdvancedPanel() );

        final JFrame frame = new JFrame( "Neo4j Desktop" );
        frame.add( root );
        frame.pack();
        frame.setResizable( false );
        frame.addWindowListener( new WindowAdapter()
        {
            @Override
            public void windowClosing( WindowEvent e )
            {
                databaseActions.shutdown();
                if ( debugWindow != null )
                {
                    debugWindow.dispose();
                }
                frame.dispose();
            }
        } );
        
        return frame;
    }

    private JPanel initAdvancedPanel()
    {
        JPanel panel = new JPanel();
        panel.setLayout( new BoxLayout( panel, Y_AXIS ) );
        panel.setVisible( false );
        
        // Heap size setting
        int initialHeapValue = heapSizeConfig.get().intValue();
        int availableTotalPhysicalMemory = getAvailableTotalPhysicalMemoryMb();
        final JSlider heapSizeSlider = new JSlider( HORIZONTAL, 0, availableTotalPhysicalMemory, initialHeapValue );
        heapSizeSlider.setEnabled( heapSizeConfig.isWritable() );
        heapSizeSlider.setToolTipText( heapSizeToolTopText() );
        heapSizeSlider.setPaintTicks( true );
        heapSizeSlider.setPaintTrack( true );
        heapSizeSlider.setPaintLabels( true );
        int majorTickSpacing = appropriateMajorTickSpacing( availableTotalPhysicalMemory );
        heapSizeSlider.setMajorTickSpacing( majorTickSpacing );
        heapSizeSlider.setMinorTickSpacing( majorTickSpacing / 2 );
        
        panel.add( heapSizeSlider );
        heapSizeSlider.addChangeListener( new ChangeListener()
        {
            @Override
            public void stateChanged( ChangeEvent e )
            {
                if ( !heapSizeSlider.getValueIsAdjusting() )
                {
                    heapSizeConfig.set( heapSizeSlider.getValue() );
                    heapSizeSlider.setToolTipText( heapSizeToolTopText() );
                }
            }
        } );
        
        return panel;
    }

    private String heapSizeToolTopText()
    {
        String text = heapSizeConfig.get() + " Mb";
        return heapSizeConfig.isWritable() ? text : text + " (not changable)";
    }

    private int appropriateMajorTickSpacing( int mb )
    {
        int rough = mb / 4;
        int digits = String.valueOf( rough ).length();
        int spacing = (int) ((int) Math.round( rough / Math.pow( 10, digits-1 ) ) * Math.pow( 10, digits-1 ));
        return spacing;
    }

    private JPanel initStatusPanel()
    {
        JPanel statusPanel = new JPanel();
        statusPanelLayout = new CardLayout();
        statusPanel.setLayout( statusPanelLayout );
        statusPanel.add( DatabaseStatus.stopped.name(), createSimpleStatusPanel(
                new Color( 1.0f, 0.5f, 0.5f ), "Choose a graph database directory, then start the server" ) );
        statusPanel.add( DatabaseStatus.starting.name(), createSimpleStatusPanel(
                new Color( 1.0f, 1.0f, 0.5f ), "In just a few seconds, Neo4j will be ready..." ) );
        statusPanel.add( DatabaseStatus.started.name(), createStartedStatus() );
        statusPanel.add( DatabaseStatus.stopping.name(), createSimpleStatusPanel(
                new Color( 0.7f, 0.7f, 0.7f ), "Neo4j is shutting down..." ) );
        statusPanel.addMouseListener( new MouseAdapter()
        {
            @Override
            public void mouseClicked( MouseEvent e )
            {
                if ( e.getButton() == MouseEvent.BUTTON1 && e.isShiftDown() && e.isControlDown() && e.isAltDown() )
                {
                    if ( debugWindow != null )
                    {
                        debugWindow.show();
                    }
                }
                else if ( e.getButton() == MouseEvent.BUTTON3 && e.isShiftDown() && e.isControlDown() && e.isAltDown() )
                {
                    toggleAdvancedMode();
                }
            }
        } );
        return statusPanel;
    }

    protected void toggleAdvancedMode()
    {
        advancedPanel.setVisible( !advancedPanel.isVisible() );
        frame.pack();
    }

    private JButton initSelectButton( final JPanel selectionPanel )
    {
        return buttonWithText( "Select database...", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                JFileChooser jFileChooser = new JFileChooser();
                jFileChooser.setFileSelectionMode( DIRECTORIES_ONLY );
                String text = directoryDisplay.getText();
                jFileChooser.setCurrentDirectory( new File( text ) );
                int result = jFileChooser.showOpenDialog( selectionPanel );

                if ( result == APPROVE_OPTION )
                {
                    File selectedFile = jFileChooser.getSelectedFile();
                    directoryDisplay.setText( selectedFile.getAbsolutePath() );
                }
            }
        } );
    }
    
    private JButton initDatabaseConfigurationButton()
    {
        JButton button = buttonWithImage( "/gear2.png", "-", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent event )
            {
                if ( Desktop.isDesktopSupported() )
                {
                    Desktop desktop = Desktop.getDesktop();
                    if ( desktop.isSupported( Desktop.Action.EDIT ) )
                    {
                        File databaseConfigurationFile = databaseConfigurationFile( getCurrentPath() );
                        try
                        {
                            ensureFileAndParentDirectoriesExists( databaseConfigurationFile );
                            desktop.edit( databaseConfigurationFile );
                        }
                        catch ( IOException e )
                        {
                            e.printStackTrace();
                            showMessageDialog( frame, "Couldn't open '" +
                                    databaseConfigurationFile.getAbsolutePath() +
                                    "', please open the file manually" );
                        }
                    }
                }
            }

            private void ensureFileAndParentDirectoriesExists( File file ) throws IOException
            {
                file.getParentFile().mkdirs();
                if ( !file.exists() )
                {
                    file.createNewFile();
                }
            }
        } );
        
        button.setBorder( new EmptyBorder( 0, 0, 0, 0 ) );
        button.setBackground( new Color( 1f, 1f, 1f, 0f ) );
        button.setFocusable( false );
        button.setPreferredSize( new Dimension( 27, 27 ) );
        return button;
    }

    private File databaseConfigurationFile( String currentPath )
    {
        return databaseActions.getDatabaseConfigurationFile( currentPath );
    }

    private JButton initStartButton()
    {
        return buttonWithText( "Start", new ActionListener()
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
                        databaseActions.start( getCurrentPath() );
                        goToStartedStatus();
                    }
                } );
            }
        } );
    }

    private JButton initStopButton()
    {
        return buttonWithText( "Stop", new ActionListener()
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
    
    private void displayStatus( DatabaseStatus state )
    {
        statusPanelLayout.show( statusPanel, state.name() );
        databaseStatus = state;
    }

    private void goToStartingStatus()
    {
        selectButton.setEnabled( false );
        databaseConfigurationButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.starting );
    }

    private void goToStartedStatus()
    {
        selectButton.setEnabled( false );
        databaseConfigurationButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( true );
        displayStatus( DatabaseStatus.started );
    }

    private void goToStoppingStatus()
    {
        selectButton.setEnabled( false );
        databaseConfigurationButton.setEnabled( false );
        startButton.setEnabled( false );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.stopping );
    }

    private void goToStoppedStatus()
    {
        selectButton.setEnabled( true );
        databaseConfigurationButton.setEnabled( true );
        startButton.setEnabled( true );
        stopButton.setEnabled( false );
        displayStatus( DatabaseStatus.stopped );
    }
    
    private static JPanel createSimpleStatusPanel( Color color, String text )
    {
        JPanel panel = new JPanel();
        panel.setBackground( color );
        JLabel label = new JLabel( text );
        panel.add( label );
        return panel;
    }

    private static JPanel createStartedStatus()
    {
        JPanel panel = createSimpleStatusPanel( new Color( 0.5f, 1.0f, 0.5f ), "Neo4j is ready. Browse to " );
        JLabel link = new JLabel( "http://localhost:7474/" );

        link.setFont( underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link ) );
        panel.add( link );
        return panel;
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private static Font underlined( Font font )
    {
        Map attributes = font.getAttributes();
        attributes.put( UNDERLINE, UNDERLINE_ON );
        return font.deriveFont( attributes );
    }

    private JButton buttonWithText( String text, ActionListener actionListener )
    {
        JButton button = new JButton( text );
        button.addActionListener( actionListener );
        return button;
    }
    
    private JButton buttonWithImage( String imageLocation, String textIfImageNotFound,
            ActionListener actionListener )
    {
        JButton button = new JButton( textIfImageNotFound );
        try
        {
            URL resource = getClass().getResource( imageLocation );
            if ( resource != null )
            {
                Image img = ImageIO.read( resource );
                button = new JButton( new ImageIcon( img ) );
            }
        }
        catch ( IOException e )
        {   // Failed to open icon. hmm
            e.printStackTrace();
        }
        
        button.addActionListener( actionListener );
        return button;
    }

    private String getCurrentPath()
    {
        return directoryDisplay.getText();
    }
}
