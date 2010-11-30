package org.neo4j.management.impl.jconsole;

import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;

class HaWidget extends Widget
{
    HaWidget( Neo4jManager manager, HighAvailability ha )
    {
    }

    @Override
    void populate( JPanel view )
    {
        view.add( new JLabel( "Neo4j is Highly Available!" ) );
    }

    @Override
    void update( Collection<UpdateEvent> result )
    {
        // TODO tobias: Implement update() [Nov 30, 2010]
    }
}
