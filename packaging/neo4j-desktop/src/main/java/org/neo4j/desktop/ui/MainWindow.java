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
import java.awt.Component;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.runtime.DatabaseActions;

import static java.awt.font.TextAttribute.UNDERLINE;
import static java.awt.font.TextAttribute.UNDERLINE_ON;
import static java.lang.String.format;
import static javax.swing.BoxLayout.X_AXIS;
import static javax.swing.BoxLayout.Y_AXIS;
import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.showConfirmDialog;
import static javax.swing.JOptionPane.showMessageDialog;
import static javax.swing.SwingConstants.HORIZONTAL;
import static javax.swing.SwingUtilities.invokeLater;
import static javax.swing.filechooser.FileSystemView.getFileSystemView;

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
    private JButton settingsButton;
    private JButton startButton;
    private JButton stopButton;
    private CardLayout statusPanelLayout;
    private JPanel statusPanel;
    private JTextField directoryDisplay;
    private JTextField configFileTextField;
    private final Value<Integer> heapSizeConfig;
    private SystemOutDebugWindow debugWindow;
    private DatabaseStatus databaseStatus; // Not used a.t.m. but may be used for something?
    private final Environment environment;
    private final Value<List<String>> extensionPackagesConfig;
    private final SysTray sysTray;

    public MainWindow( final DatabaseActions databaseActions, Environment environment,
            Value<Integer> heapSizeConfig, Value<List<String>> extensionPackagesConfig )
    {
        // This is here only for debugging, comment out the line below to see system out
//        debugWindow = new SystemOutDebugWindow();
        
        this.environment = environment;
        this.heapSizeConfig = heapSizeConfig;
        this.databaseActions = databaseActions;
        this.extensionPackagesConfig = extensionPackagesConfig;

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
        return buttonWithText( elipsis( "Settings"), new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                JDialog settingsDialog = new SettingsDialog( frame );
                settingsDialog.setVisible( true );
            }
        } );
    }

    private String elipsis(  String input )
    {
        return format( "%s\u2026", input );
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

    private JPanel initSettingsPanel()
    {
        JPanel settingsPanel = new JPanel();
        settingsPanel.setLayout( new BoxLayout( settingsPanel, Y_AXIS ) );
        settingsPanel.setBorder( BorderFactory.createTitledBorder( "Settings" ) );

        settingsPanel.add( initEditConfigPanel() );
        settingsPanel.add( initEditVmOptionsPanel() );
        settingsPanel.add( initHeapSizePanel() );
        settingsPanel.add( initExtensionsPanel() );
        
        return settingsPanel;
    }

    private Component initEditConfigPanel()
    {
        JPanel panel = new JPanel();
        panel.setLayout( new FlowLayout() );
        panel.setBorder( BorderFactory.createTitledBorder( "Configuration" ) );
        configFileTextField = new JTextField( getDatabaseConfigurationFile().getAbsolutePath(), 30 );
        configFileTextField.setEditable( false );
        panel.add( configFileTextField );
        panel.add( initEditDatabaseConfigurationButton() );
        return panel;
    }

    private Component initEditVmOptionsPanel()
    {
        JPanel panel = new JPanel();
        panel.setLayout( new FlowLayout() );
        panel.setBorder( BorderFactory.createTitledBorder( "VM Options" ) );
        File vmOptionsFile = getVmOptionsFile();
        JTextField vmOptionsFileTextField =
            new JTextField( vmOptionsFile == null ? "" : vmOptionsFile.getAbsolutePath() );
        vmOptionsFileTextField.setEditable( false );
        panel.add( vmOptionsFileTextField );
        panel.add( initEditVmOptionsButton() );
        return panel;
    }

    private File getVmOptionsFile()
    {
        try
        {
            File jarFile = new File(MainWindow.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return new File( jarFile.getParentFile(), ".vmoptions" );
        }
        catch ( URISyntaxException e )
        {
            if ( debugWindow != null )
            {
                e.printStackTrace( System.out );
            }
        }
        return null;
    }

    private File getDatabaseConfigurationFile()
    {
        return databaseActions.getDatabaseConfigurationFile( getCurrentPath() );
    }

    private JPanel initExtensionsPanel()
    {
        // Extensions packages config
        final DefaultComboBoxModel<String> extensionPackagesModel = new DefaultComboBoxModel<>(
                extensionPackagesConfig.get().toArray( new String[0] ) );
        final JComboBox<String> extensionPackages = new JComboBox<>( extensionPackagesModel );
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
        packagesPanel.setBorder( BorderFactory.createTitledBorder( "Server Extensions" ) );
        packagesPanel.add( new HeadlinePanel( "Extension packages for " +
                environment.getExtensionsDirectory().getAbsolutePath() + ")" ) );
        JPanel packagesComponentsPanel = new JPanel();
        packagesComponentsPanel.setLayout( new BoxLayout( packagesComponentsPanel, X_AXIS ) );
        packagesComponentsPanel.add( extensionPackages );
        packagesComponentsPanel.add( addPackageButton );
        packagesComponentsPanel.add( removePackageButton );
        packagesPanel.add( packagesComponentsPanel );
        return packagesPanel;
    }

    private JPanel initHeapSizePanel()
    {
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
        heapSizePanel.add( new HeadlinePanel( "Heap size (changes requires restart)" ) );
        heapSizePanel.add( heapSizeSlider );
        return heapSizePanel;
    }

    private List<String> itemsAsList( DefaultComboBoxModel<String> model )
    {
        List<String> list = new ArrayList<>( model.getSize() );
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
        statusPanel.setBorder( BorderFactory.createTitledBorder( "Status" ) );
        statusPanelLayout = new CardLayout();
        statusPanel.setLayout( statusPanelLayout );
        statusPanel.add( DatabaseStatus.stopped.name(), createSimpleStatusPanel(
                new Color( 1.0f, 0.5f, 0.5f ), "Choose a graph database directory, then start the server" ) );
        statusPanel.add( DatabaseStatus.starting.name(), createSimpleStatusPanel(
                new Color( 1.0f, 1.0f, 0.5f ), elipsis( "In just a few seconds, Neo4j will be ready" ) ) );
        statusPanel.add( DatabaseStatus.started.name(), createStartedStatus() );
        statusPanel.add( DatabaseStatus.stopping.name(), createSimpleStatusPanel(
                new Color( 0.7f, 0.7f, 0.7f ), elipsis( "Neo4j is shutting down" ) ) );
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
        return buttonWithText( elipsis( "Browse" ), new ActionListener()
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
                    switch ( jFileChooser.showOpenDialog( selectionPanel ) ) {
                        default:
                            return;

                        case APPROVE_OPTION:
                            File selectedFile = jFileChooser.getSelectedFile();

                            try
                            {
                                verifyGraphDirectory( selectedFile );
                                directoryDisplay.setText( selectedFile.getAbsolutePath() );
                                configFileTextField.setText( getDatabaseConfigurationFile().getAbsolutePath() );
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


    private JButton initEditDatabaseConfigurationButton()
    {
        return buttonWithText( elipsis( "Edit" ), new EditFileActionListener()
        {
            @Override
            protected File getFile()
            {
                return getDatabaseConfigurationFile();
            }
        } );
    }

    private JButton initEditVmOptionsButton()
    {
        return buttonWithText( elipsis( "Edit" ), new EditFileActionListener()
        {
            @Override
            protected File getFile()
            {
                return getVmOptionsFile();
            }
        } );
    }

    private abstract class EditFileActionListener implements ActionListener {

        protected abstract File getFile();

        @Override
        public void actionPerformed( ActionEvent event )
        {
            File file = getFile();
            if ( null == file )
            {
                showMessageDialog( frame,
                    "Did not find location of .vmoptions file",
                    "Error",
                    ERROR_MESSAGE );
            }
            try
            {
                ensureFileAndParentDirectoriesExists( file );
                environment.editFile( file );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                showMessageDialog( frame,
                    format("Couldn't open %s, please open the file manually", file.getAbsolutePath() ),
                    "Error",
                    ERROR_MESSAGE );
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
                        String path = getCurrentPath();
                        databaseActions.start( path );
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
        link.setFont( underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link, environment ) );

        return createStatusPanel( new Color( 0.5f, 1.0f, 0.5f ), new JLabel("Neo4j is ready. Browse to "), link );
    }

    private JPanel createStatusPanel( Color color, JComponent... components )
    {
        JPanel panel = new JPanel();
        panel.setLayout( new FlowLayout(  ) );
        panel.setBackground( color );
        for ( JComponent component : components )
        {
            panel.add( component );
        }
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

    private String getCurrentPath()
    {
        return directoryDisplay.getText();
    }

    private class UnsuitableGraphDatabaseDirectory extends Exception
    {
        private final File dir;

        UnsuitableGraphDatabaseDirectory( String message, File dir )
        {
            super( format( message, dir.getAbsolutePath() ) );
            this.dir = dir;
        }
    }

    private class SettingsDialog extends JDialog
    {
        SettingsDialog( Frame owner )
        {
            super( owner, "Neo4j Settings", true );


            Container dialogContainer = getContentPane();
            JPanel content = new JPanel();
            content.setLayout( new BoxLayout( content, Y_AXIS ) );
            content.setBorder( BorderFactory.createEmptyBorder( 5, 5, 5, 5 ) );

            JPanel actions = new JPanel();
            actions.setLayout( new FlowLayout( FlowLayout.RIGHT ) );
            actions.add( buttonWithText( "Close", new ActionListener()
            {
                @Override
                public void actionPerformed( ActionEvent e )
                {
                    close();
                }
            } ) );


            content.add( initEditConfigPanel() );
            content.add( initEditVmOptionsPanel() );
            content.add( initExtensionsPanel() );
            content.add( Box.createVerticalStrut( 5 ) );
            content.add( actions );

            dialogContainer.add( content );

            pack();
        }

        private void close()
        {
            setVisible( false );
        }
    }
}
