package org.neo4j.unsafe.impl.batchimport.input;

import java.util.ArrayList;
import java.util.List;

public class CachingInputEntityVisitor implements InputEntityVisitor
{
    public boolean hasPropertyId;
    public long propertyId;
    public final List<Object> properties = new ArrayList<>();

    public boolean hasLongId;
    public long longId;
    public String stringId;
    public Group idGroup;

    public final List<String> labels = new ArrayList<>();
    public boolean hasLabelField;
    public long labelField;

    public boolean hasLongStartId;
    public long longStartId;
    public String stringStartId;
    public Group startIdGroup;

    public boolean hasLongEndId;
    public long longEndId;
    public String stringEndId;
    public Group endIdGroup;

    public boolean hasIntType;
    public int intType;
    public String stringType;

    @Override
    public boolean propertyId( long nextProp )
    {
        hasPropertyId = true;
        propertyId = nextProp;
        return true;
    }

    @Override
    public boolean property( String key, Object value )
    {
        properties.add( key );
        properties.add( value );
        return true;
    }

    @Override
    public boolean id( long id, Group group )
    {
        hasLongId = true;
        longId = id;
        idGroup = group;
        return true;
    }

    @Override
    public boolean id( String id, Group group )
    {
        stringId = id;
        idGroup = group;
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        for ( String label : labels )
        {
            this.labels.add( label );
        }
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        hasLabelField = true;
        this.labelField = labelField;
        return true;
    }

    @Override
    public boolean startId( long id, Group group )
    {
        hasLongStartId = true;
        longStartId = id;
        startIdGroup = group;
        return true;
    }

    @Override
    public boolean startId( String id, Group group )
    {
        stringStartId = id;
        startIdGroup = group;
        return true;
    }

    @Override
    public boolean endId( long id, Group group )
    {
        hasLongEndId = true;
        longEndId = id;
        endIdGroup = group;
        return true;
    }

    @Override
    public boolean endId( String id, Group group )
    {
        stringEndId = id;
        endIdGroup = group;
        return true;
    }

    @Override
    public boolean type( int type )
    {
        hasIntType = true;
        intType = type;
        return true;
    }

    @Override
    public boolean type( String type )
    {
        stringType = type;
        return true;
    }

    @Override
    public void endOfEntity()
    {
        hasPropertyId = false;
        properties.clear();
        hasLongId = false;
        labels.clear();
        hasLabelField = false;
        hasLongStartId = false;
        hasLongEndId = false;
        hasIntType = false;
    }
}
