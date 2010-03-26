package org.neo4j.graphmatching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;

public abstract class AbstractPatternObject<T extends PropertyContainer>
{
    private T assocication;
    private Map<String, Collection<ValueMatcher>> constrains =
            new HashMap<String, Collection<ValueMatcher>>();

    public void addPropertyConstraint( String propertyKey, ValueMatcher matcher )
    {
        Collection<ValueMatcher> matchers = this.constrains.get( propertyKey );
        if ( matchers == null )
        {
            matchers = new ArrayList<ValueMatcher>();
            this.constrains.put( propertyKey, matchers );
        }
        matchers.add( matcher );
    }
    
    public void setAssociation( T object )
    {
        this.assocication = object;
    }

    public T getAssociation()
    {
        return this.assocication;
    }

    public Iterable<Map.Entry<String, Collection<ValueMatcher>>> getPropertyConstraints()
    {
        Iterable<Map.Entry<String, Collection<ValueMatcher>>> matchers = this.constrains.entrySet();
        return matchers != null ? matchers :
                Collections.<Map.Entry<String, Collection<ValueMatcher>>>emptyList();
    }
}
