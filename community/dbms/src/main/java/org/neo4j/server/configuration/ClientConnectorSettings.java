package org.neo4j.server.configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.singletonList;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.GroupSettingSupport.enumerate;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.legacyFallback;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

public class ClientConnectorSettings
{
    public static HttpConnector httpConnector( String key )
    {
        return new HttpConnector( key, HttpConnector.Encryption.NONE );
    }

    public static Optional<HttpConnector> httpConnector( Config config, HttpConnector.Encryption encryption )
    {
        List<HttpConnector> httpConnectors = config
                .view( enumerate( GraphDatabaseSettings.Connector.class ) )
                .map( ( key ) -> new HttpConnector( key, encryption ) )
                .filter( ( connConfig ) -> connConfig.group.groupKey.equals( encryption.uriScheme ) ||
                        (config.get( connConfig.type ) == HTTP && config.get( connConfig.encryption ) == encryption) )
                .collect( Collectors.toList() );
        if ( httpConnectors.isEmpty() )
        {
            httpConnectors = singletonList( new HttpConnector( encryption ) );
        }

        return httpConnectors.stream()
                .filter( ( connConfig ) -> config.get( connConfig.enabled ) )
                .findFirst();
    }

    @Description("Configuration options for HTTP connectors. " +
            "\"(http-connector-key)\" is a placeholder for a unique name for the connector, for instance " +
            "\"http-public\" or some other name that describes what the connector is for.")
    public static class HttpConnector extends GraphDatabaseSettings.Connector
    {
        @Description("Enable TLS for this connector")
        public final Setting<Encryption> encryption;

        @Description( "Address the connector should bind to. " +
                "This setting is deprecated and will be replaced by `+listen_address+`" )
        public final Setting<ListenSocketAddress> address;

        @Description( "Address the connector should bind to" )
        public final Setting<ListenSocketAddress> listen_address;

        @Description( "Advertised address for this connector" )
        public final Setting<AdvertisedSocketAddress> advertised_address;

        public HttpConnector()
        {
            this( Encryption.NONE );
        }

        public HttpConnector( Encryption encryptionLevel )
        {
            this( "(http-connector-key)", encryptionLevel );
        }

        public HttpConnector( String key, Encryption encryptionLevel )
        {
            super( key, null );
            encryption = group.scope( setting( "encryption", options( HttpConnector.Encryption.class ), NO_DEFAULT ) );
            Setting<ListenSocketAddress> legacyAddressSetting = listenAddress( "address", encryptionLevel.defaultPort );
            Setting<ListenSocketAddress> listenAddressSetting = legacyFallback( legacyAddressSetting,
                    listenAddress( "listen_address", encryptionLevel.defaultPort ) );

            this.address = group.scope( legacyAddressSetting );
            this.listen_address = group.scope( listenAddressSetting );
            this.advertised_address = group.scope( advertisedAddress( "advertised_address", listenAddressSetting ) );
        }

        public enum Encryption
        {
            NONE( "http", 7474 ), TLS( "https", 7473 );

            final String uriScheme;
            final int defaultPort;

            Encryption( String uriScheme, int defaultPort )
            {
                this.uriScheme = uriScheme;
                this.defaultPort = defaultPort;
            }
        }
    }
}
