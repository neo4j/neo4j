package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
public class RelationshipImplTest
{
    @Parameters
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<Object[]>();
        for ( int i = 1; i <= 16; i++ )
        {
            data.add( new Object[] { (1 << i) - 1 } );
        }
        return data;
    }
    
    private final int typeId;

    public RelationshipImplTest( int typeId )
    {
        this.typeId = typeId;
    }
    
    @Test
    public void typeIdCanUse16Bits()
    {
        RelationshipImpl rel = new RelationshipImpl( 10, 10, 10, typeId, true );
        assertEquals( typeId, rel.getTypeId() );
    }
}
