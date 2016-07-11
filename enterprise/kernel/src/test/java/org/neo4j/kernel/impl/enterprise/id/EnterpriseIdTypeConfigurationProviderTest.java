package org.neo4j.kernel.impl.enterprise.id;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class EnterpriseIdTypeConfigurationProviderTest
{

    private IdType reusableType;

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        return Arrays.asList( new Object[]{IdType.PROPERTY},
                new Object[]{IdType.STRING_BLOCK},
                new Object[]{IdType.ARRAY_BLOCK},
                new Object[]{IdType.RELATIONSHIP},
                new Object[]{IdType.NODE_LABELS} );
    }

    public EnterpriseIdTypeConfigurationProviderTest( IdType reusableType )
    {
        this.reusableType = reusableType;
    }

    @Test
    public void nonReusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( IdType.NODE );
        assertFalse( "Node ids are not reusable.", typeConfiguration.allowAggressiveReuse() );
        assertEquals( "Node ids are not reusable.", 1024, typeConfiguration.getGrabSize() );
    }

    @Test
    public void reusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( reusableType );
        assertTrue( typeConfiguration.allowAggressiveReuse() );
        assertEquals( 50000, typeConfiguration.getGrabSize() );
    }

    private IdTypeConfigurationProvider createIdTypeProvider()
    {
        Map<String,String> params = MapUtil.stringMap( EnterpriseEditionSettings.idTypesToReuse.name(),
                IdType.RELATIONSHIP.name() );
        Config config = new Config( params );
        return new EnterpriseIdTypeConfigurationProvider( config );
    }

}