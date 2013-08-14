package org.neo4j.desktop.ui;

import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.desktop.config.Environment;

import static org.neo4j.desktop.ui.Components.createPanel;
import static org.neo4j.desktop.ui.Components.elipsis;
import static org.neo4j.desktop.ui.Components.withBackground;
import static org.neo4j.desktop.ui.Components.withLayout;

public enum DatabaseStatus
{
    STOPPED,
    STARTING,
    STARTED,
    STOPPING;
    public static final Color STOPPED_COLOR = new Color( 1.0f, 0.5f, 0.5f );
    public static final Color CHANGING_COLOR = new Color( 1.0f, 1.0f, 0.5f );
    public static final Color STARTED_COLOR = new Color( 0.5f, 1.0f, 0.5f );

    public Component display( Environment environment )
    {
        switch ( this )
        {
            case STOPPED:
                return createTextStatusDisplay( STOPPED_COLOR, "Choose a graph database directory, " +
                        "then start the server" );
            case STARTING:
                return createTextStatusDisplay( CHANGING_COLOR, elipsis( "In just a few seconds, Neo4j will be ready" ) );
            case STARTED:
                return createStartedStatusDisplay( environment );
            case STOPPING:
                return createTextStatusDisplay( CHANGING_COLOR, elipsis( "Neo4j is shutting down" ) );
            default:
                throw new IllegalStateException();
        }
    }

    private static JPanel createTextStatusDisplay( Color color, String text )
    {
        return createStatusDisplay( color, new JLabel( text ) );
    }

    private static JPanel createStartedStatusDisplay( Environment environment )
    {
        JLabel link = new JLabel( "http://localhost:7474/" );
        link.setFont( Components.underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link, environment ) );

        return createStatusDisplay( STARTED_COLOR, new JLabel( "Neo4j is ready. Browse to " ), link );
    }

    private static JPanel createStatusDisplay( Color color, Component... components )
    {
        return withBackground( color, withLayout( new FlowLayout(), createPanel( components ) ) );
    }
}
