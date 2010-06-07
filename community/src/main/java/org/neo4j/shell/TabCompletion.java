package org.neo4j.shell;

import java.io.Serializable;
import java.util.Collection;

public class TabCompletion implements Serializable
{
    private final Collection<String> candidates;
    private final int cursor;

    public TabCompletion( Collection<String> candidates, int cursor )
    {
        this.candidates = candidates;
        this.cursor = cursor;
    }

    public Collection<String> getCandidates()
    {
        return candidates;
    }

    public int getCursor()
    {
        return cursor;
    }
}
