package org.neo4j.desktop.ui;

import java.awt.Component;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.neo4j.desktop.config.Environment;

import static javax.swing.BoxLayout.X_AXIS;
import static javax.swing.BoxLayout.Y_AXIS;

import static org.neo4j.desktop.ui.Components.buttonWithText;
import static org.neo4j.desktop.ui.Components.elipsis;

class SettingsDialog extends JDialog
{
    private final Environment environment;
    private final DesktopModel model;

    SettingsDialog( Frame owner, Environment environment, DesktopModel model )
    {
        super( owner, "Neo4j Settings", true );
        this.environment = environment;
        this.model = model;

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

    private Component initEditConfigPanel()
    {
        JPanel panel = new JPanel();
        panel.setLayout( new FlowLayout() );
        panel.setBorder( BorderFactory.createTitledBorder( "Configuration" ) );
        JTextField configFileTextField = new JTextField( model.getDatabaseConfigurationFile().getAbsolutePath(), 30 );
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
        File vmOptionsFile = model.getVmOptionsFile();
        JTextField vmOptionsFileTextField =
                new JTextField( vmOptionsFile == null ? "" : vmOptionsFile.getAbsolutePath() );
        vmOptionsFileTextField.setEditable( false );
        panel.add( vmOptionsFileTextField );
        panel.add( initEditVmOptionsButton() );
        return panel;
    }

    private JPanel initExtensionsPanel()
    {
        // Extensions packages config
        final DefaultComboBoxModel<String> extensionPackagesModel =
                new DefaultComboBoxModel<>( model.getExtensionPackagesConfigAsArray() );
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
                    model.setExtensionPackagesConfig( itemsAsList( extensionPackagesModel ) );
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
                    model.setExtensionPackagesConfig( itemsAsList( extensionPackagesModel ) );
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

    private List<String> itemsAsList( DefaultComboBoxModel<String> model )
    {
        List<String> list = new ArrayList<>( model.getSize() );
        for ( int i = 0; i < model.getSize(); i++ )
        {
            list.add( model.getElementAt( i ) );
        }
        return list;
    }

    private JButton initEditDatabaseConfigurationButton()
    {
        return Components.buttonWithText( elipsis( "Edit" ), new EditFileActionListener( this, environment )
        {
            @Override
            protected File getFile()
            {
                return model.getDatabaseConfigurationFile();
            }
        } );
    }

    private JButton initEditVmOptionsButton()
    {
        return Components.buttonWithText( elipsis( "Edit" ), new EditFileActionListener( this, environment )
        {
            @Override
            protected File getFile()
            {
                return model.getVmOptionsFile();
            }
        } );
    }
}
