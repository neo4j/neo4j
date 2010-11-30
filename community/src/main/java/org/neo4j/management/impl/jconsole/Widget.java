package org.neo4j.management.impl.jconsole;

import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JPanel;

abstract class Widget
{
    final JPanel view()
    {
        JPanel panel = new JPanel();
        populate( panel );
        return panel;
    }

    abstract void populate( JPanel view );

    void dispose()
    {
    }

    final UpdateEvent[] update()
    {
        Collection<UpdateEvent> result = new ArrayList<UpdateEvent>();
        update( result );
        return result.isEmpty() ? null : result.toArray( new UpdateEvent[result.size()] );
    }

    abstract void update( Collection<UpdateEvent> result );
}
