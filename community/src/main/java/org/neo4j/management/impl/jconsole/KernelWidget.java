package org.neo4j.management.impl.jconsole;

import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.management.Neo4jManager;

class KernelWidget extends Widget
{
    KernelWidget( Neo4jManager manager )
    {
    }

    @Override
    void populate( JPanel view )
    {
        view.add( new JLabel( "Neo4j Rocks!" ) );
    }

    @Override
    void update( Collection<UpdateEvent> result )
    {
        // TODO tobias: Implement update() [Nov 30, 2010]
    }
}
