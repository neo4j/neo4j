package org.neo4j.kernel.impl.enterprise.id;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;

/**
 * Id type configuration provider for enterprise edition.
 * Allow to reuse predefined id types that are reused in community and in
 * addition to that allow additional id types to reuse be specified by
 * {@link EnterpriseEditionSettings#idTypesToReuse} setting.
 * @see IdType
 * @see IdTypeConfiguration
 */
public class EnterpriseIdTypeConfigurationProvider extends CommunityIdTypeConfigurationProvider
{

    private final Set<IdType> typesToReuse;

    public EnterpriseIdTypeConfigurationProvider(Config config)
    {
        typesToReuse = configureReusableTypes( config );
    }

    @Override
    protected Set<IdType> getTypesToReuse()
    {
        return typesToReuse;
    }

    private EnumSet<IdType> configureReusableTypes( Config config )
    {
        List<String> typeNames = config.get( EnterpriseEditionSettings.idTypesToReuse );
        List<IdType> idTypes = new ArrayList<>();
        for ( String idType : typeNames )
        {
            idTypes.add( IdType.valueOf( idType ) );
        }

        EnumSet<IdType> types = EnumSet.copyOf( super.getTypesToReuse() );
        types.addAll( idTypes );
        return types;
    }
}
