package org.neo4j.onlinebackup.net;

public interface Callback
{
    public void jobExecuted( Job job );
}
