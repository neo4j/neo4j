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
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.border.BevelBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.runtime.DatabaseActions;

import static java.awt.font.TextAttribute.UNDERLINE;
import static java.awt.font.TextAttribute.UNDERLINE_ON;

import static javax.swing.BoxLayout.X_AXIS;
import static javax.swing.BoxLayout.Y_AXIS;
import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.showMessageDialog;
import static javax.swing.SwingConstants.HORIZONTAL;
import static javax.swing.SwingUtilities.invokeLater;

import static org.neo4j.desktop.config.OsSpecificHeapSizeConfig.getAvailableTotalPhysicalMemoryMb;
import static org.neo4j.desktop.ui.UIHelper.loadImage;

/**
 * The main window of the Neo4j Desktop. Able to start/stop a database as well as providing access to some
 * advanced configuration options, such as heap size and database properties.
 */
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
    private SystemOutDebugWindow debugWindow;
    private DatabaseStatus databaseStatus; // Not used a.t.m. but may be used for something?
    private final Environment environment;
    private final Value<List<String>> extensionPackagesConfig;
    private final SysTray sysTray;

    public MainWindow( final DatabaseActions databaseActions, Environment environment,
            Value<Integer> heapSizeConfig, Value<List<String>> extensionPackagesConfig )
    {
        // TODO (keep the below line in for easier getting going with debugging)
        //      Only for debugging, comment out the line below for real usage
//        debugWindow = new SystemOutDebugWindow();
        
        this.environment = environment;
        this.heapSizeConfig = heapSizeConfig;
        this.databaseActions = databaseActions;
        this.extensionPackagesConfig = extensionPackagesConfig;

        frame = init();
        
        this.sysTray = SysTray.install( "/neo4j-db-16.png", new SysTray.Actions()
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
        
        return frame;
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

    private JPanel initAdvancedPanel()
    {
        JPanel advancedPanel = new JPanel();
        advancedPanel.setLayout( new BoxLayout( advancedPanel, Y_AXIS ) );
        advancedPanel.setVisible( false );
        advancedPanel.add( headLinePanel( "Advanced settings" ) );
        
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
        JPanel heapSizePanel = new JPanel();
        heapSizePanel.setLayout( new BoxLayout( heapSizePanel, Y_AXIS ) );
        heapSizePanel.add( headLinePanel( "Heap size (changes requires restart)" ) );
        heapSizePanel.add( heapSizeSlider );
        advancedPanel.add( heapSizePanel );
        
        // Extensions packages config
        final DefaultComboBoxModel<String> extensionPackagesModel = new DefaultComboBoxModel<String>(
                extensionPackagesConfig.get().toArray( new String[0] ) );
        final JComboBox<String> extensionPackages = new JComboBox<String>( extensionPackagesModel );
        JButton addPackageButton = buttonWithText( "+", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                String newPackage = JOptionPane.showInputDialog( "Package containing extension(s) to include" );
                if ( newPackage != null )
                {
                    extensionPackagesModel.addElement( newPackage );
                    extensionPackagesConfig.set( itemsAsList( extensionPackagesModel ) );
                }
            }
        } );
        JButton removePackageButton = buttonWithText( "-", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                int selectedIndex = extensionPackages.getSelectedIndex();
                if ( selectedIndex != -1 )
                {
                    extensionPackagesModel.removeElementAt( selectedIndex );
                    extensionPackagesConfig.set( itemsAsList( extensionPackagesModel ) );
                }
            }
        } );
        JPanel packagesPanel = new JPanel();
        packagesPanel.setLayout( new BoxLayout( packagesPanel, Y_AXIS ) );
        packagesPanel.add( headLinePanel( "Extension packages for " +
                environment.getExtensionsDirectory().getAbsolutePath() + ")" ) );
        JPanel packagesComponentsPanel = new JPanel();
        packagesComponentsPanel.setLayout( new BoxLayout( packagesComponentsPanel, X_AXIS ) );
        packagesComponentsPanel.add( extensionPackages );
        packagesComponentsPanel.add( addPackageButton );
        packagesComponentsPanel.add( removePackageButton );
        packagesPanel.add( packagesComponentsPanel );
        advancedPanel.add( packagesPanel );
        
        return advancedPanel;
    }

    private JPanel headLinePanel( String headline )
    {
        JPanel panel = new JPanel();
        panel.setLayout( new BoxLayout( panel, Y_AXIS ) );
        panel.add( new JSeparator() );
        JPanel labelPanel = new JPanel();
        labelPanel.setLayout( new FlowLayout() );
        labelPanel.add( new JLabel( headline ) );
        panel.add( labelPanel );
        return panel;
    }

    private List<String> itemsAsList( DefaultComboBoxModel<String> model )
    {
        List<String> list = new ArrayList<String>( model.getSize() );
        for ( int i = 0; i < model.getSize(); i++ )
        {
            list.add( model.getElementAt( i ) );
        }
        return list;
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
                if ( e.getButton() == MouseEvent.BUTTON1 )
                {
                    toggleAdvancedMode();
                }
                else if ( e.getButton() == MouseEvent.BUTTON3 && debugWindow != null &&
                        e.isShiftDown() && e.isControlDown() && e.isAltDown() )
                {
                    debugWindow.show();
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
        JButton button = buttonWithImage( "/gear.png", "-", new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent event )
            {
                File databaseConfigurationFile = databaseActions.getDatabaseConfigurationFile( getCurrentPath() );
                try
                {
                    ensureFileAndParentDirectoriesExists( databaseConfigurationFile );
                    environment.editFile( databaseConfigurationFile );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                    showMessageDialog( frame, "Couldn't open '" +
                            databaseConfigurationFile.getAbsolutePath() +
                            "', please open the file manually" );
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
        
        button.setBorder( new BevelBorder( BevelBorder.RAISED ) );
        button.setFocusable( false );
        button.setPreferredSize( new Dimension( 27, 27 ) );
        return button;
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
    
    private void displayStatus( DatabaseStatus status )
    {
        statusPanelLayout.show( statusPanel, status.name() );
        databaseStatus = status;
        sysTray.changeStatus( status );
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

    private JPanel createStartedStatus()
    {
        JPanel panel = createSimpleStatusPanel( new Color( 0.5f, 1.0f, 0.5f ), "Neo4j is ready. Browse to " );
        JLabel link = new JLabel( "http://localhost:7474/" );

        link.setFont( underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link, environment ) );
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
        JButton button = new JButton();
        try
        {
            button.setIcon( new ImageIcon( loadImage( imageLocation ) ) );
        }
        catch ( Exception e1 )
        {
            button.setText( textIfImageNotFound );
        }
        
        button.addActionListener( actionListener );
        return button;
    }

    private String getCurrentPath()
    {
        return directoryDisplay.getText();
    }
}
