package org.neo4j.remote.sites;

import java.util.Collection;
import java.util.LinkedList;

import org.neo4j.api.core.NeoService;
import org.neo4j.util.index.IndexService;

class NeoServiceContainer implements Runnable
{
    final NeoService service;
    private final Collection<IndexService> indexServices = new LinkedList<IndexService>();

    NeoServiceContainer( NeoService neo )
    {
        this.service = neo;
    }

    void addIndexService( IndexService index )
    {
        this.indexServices.add( index );
    }

    /*
     * This container is usable as a shutdown hook for the NeoService it contains
     */
    public void run()
    {
        for ( IndexService index : indexServices )
        {
            index.shutdown();
        }
        this.service.shutdown();
    }
}
