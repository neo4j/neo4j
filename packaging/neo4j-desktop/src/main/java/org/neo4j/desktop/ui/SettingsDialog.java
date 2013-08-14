package org.neo4j.desktop.ui;

import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.swing.BorderFactory;
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
        // Extensions packages config
        final DefaultListModel<String> packageListModel = new DefaultListModel<>();
        for ( String packageName : model.getExtensionPackagesConfig() )
        {
            packageListModel.addElement( packageName );
        }

        final JList<String> packageList = new JList<>( packageListModel );
        JPanel packageListButtons = withBoxLayout( BoxLayout.Y_AXIS, createPanel(
                createTextButton( "+", new ActionListener()
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
                } ),
                createTextButton( "-", new ActionListener()
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
                } )
        ) );

        return withBorder(
            BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder(
                    format( "Server Extensions (at %s)", environment.getExtensionsDirectory().getAbsolutePath() ) ),
                    createSpacingBorder( 2 ) ),
            withBoxLayout( BoxLayout.X_AXIS, createPanel(
                new JScrollPane( packageList ), createHorizontalSpacing(), packageListButtons ) ) );
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
