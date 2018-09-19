/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.kubernetes.KubernetesType;
import org.neo4j.causalclustering.discovery.kubernetes.ServiceList;
import org.neo4j.causalclustering.discovery.kubernetes.Status;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.ssl.PkiUtils;

import static org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.neo4j.util.Preconditions.checkState;

public class KubernetesResolver implements RemoteMembersResolver
{
    private final KubernetesClient kubernetesClient;
    private final HttpClient httpClient;
    private final Log log;

    private KubernetesResolver( LogService logService, Config config )
    {
        this.log = logService.getInternalLog( getClass() );

        SslContextFactory sslContextFactory = createSslContextFactory( config );
        this.httpClient = new HttpClient( sslContextFactory );

        String token = read( config.get( CausalClusteringSettings.kubernetes_token ) );
        String namespace = read( config.get( CausalClusteringSettings.kubernetes_namespace ) );

        this.kubernetesClient = new KubernetesClient( logService, httpClient, token, namespace, config,
                RetryingHostnameResolver.defaultRetryStrategy( config, logService.getInternalLogProvider() ) );
    }

    public static RemoteMembersResolver resolver( LogService logService, Config config )
    {
        return new KubernetesResolver( logService, config );
    }

    private SslContextFactory createSslContextFactory( Config config )
    {
        File caCert = config.get( CausalClusteringSettings.kubernetes_ca_crt );
        try (
                SecurePassword password = new SecurePassword( 16, new SecureRandom() );
                InputStream caCertStream = Files.newInputStream( caCert.toPath(), StandardOpenOption.READ )
        )
        {
            KeyStore keyStore = loadKeyStore( password, caCertStream );

            SslContextFactory sslContextFactory = new SslContextFactory();
            sslContextFactory.setTrustStore( keyStore );
            sslContextFactory.setTrustStorePassword( String.valueOf( password.password() ) );

            return sslContextFactory;
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Unable to load CA certificate for Kubernetes", e );
        }
    }

    private KeyStore loadKeyStore( SecurePassword password, InputStream caCertStream )
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException
    {
        CertificateFactory certificateFactory = CertificateFactory.getInstance( PkiUtils.CERTIFICATE_TYPE );
        Collection<? extends Certificate> certificates = certificateFactory.generateCertificates( caCertStream );
        checkState( !certificates.isEmpty(), "Expected non empty Kubernetes CA certificates" );
        KeyStore keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
        keyStore.load( null, password.password() );

        int idx = 0;
        for ( Certificate certificate : certificates )
        {
            keyStore.setCertificateEntry( "ca" + idx++, certificate );
        }
        return keyStore;
    }

    private String read( File file )
    {
        try
        {
            Optional<String> line = Files.lines( file.toPath() ).findFirst();

            if ( line.isPresent() )
            {
                return line.get();
            }
            else
            {
                throw new IllegalStateException( String.format( "Expected file at %s to have at least 1 line", file ) );
            }
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( "Unable to read file " + file, e );
        }
    }

    @Override
    public <T> Collection<T> resolve( Function<AdvertisedSocketAddress,T> transform )
    {
        try
        {
            httpClient.start();
            return kubernetesClient.resolve( null ).stream().map( transform ).collect( Collectors.toList() );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Unable to query Kubernetes API", e );
        }
        finally
        {
            try
            {
                httpClient.stop();
            }
            catch ( Exception e )
            {
                log.warn( "Unable to shut down HTTP client", e );
            }
        }
    }

    /**
    * Interface requires a parameter for resolve() and resolveOnce() that is unused here. This boils down to the interface of RetryStrategy, which has
    * an INPUT type parameter. Not worth duplicating RetryStrategy for this one class.
    * See <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-service-v1-core">List Service</a>
    */
     static class KubernetesClient extends RetryingHostnameResolver
    {
        static final String path = "/api/v1/namespaces/%s/services";
        private final Log log;
        private final Log userLog;
        private final HttpClient httpClient;
        private final String token;
        private final String namespace;
        private final String labelSelector;
        private final ObjectMapper objectMapper;
        private final String portName;
        private final AdvertisedSocketAddress kubernetesAddress;

        KubernetesClient( LogService logService, HttpClient httpClient, String token, String namespace,
                Config config, MultiRetryStrategy<AdvertisedSocketAddress,Collection<AdvertisedSocketAddress>> retryStrategy )
        {
            super( config, retryStrategy );
            this.log = logService.getInternalLog( getClass() );
            this.userLog = logService.getUserLog( getClass() );
            this.token = token;
            this.namespace = namespace;

            this.kubernetesAddress = config.get( CausalClusteringSettings.kubernetes_address );
            this.labelSelector = config.get( CausalClusteringSettings.kubernetes_label_selector );
            this.portName = config.get( CausalClusteringSettings.kubernetes_service_port_name );

            this.httpClient = httpClient;
            this.objectMapper = new ObjectMapper().configure( FAIL_ON_UNKNOWN_PROPERTIES, false );
        }

        @Override
        protected Collection<AdvertisedSocketAddress> resolveOnce( AdvertisedSocketAddress ignored )
        {
            try
            {
                ContentResponse response = httpClient
                        .newRequest( kubernetesAddress.getHostname(), kubernetesAddress.getPort() )
                        .method( HttpMethod.GET )
                        .scheme( "https" )
                        .path( String.format( path, namespace ) )
                        .param( "labelSelector", labelSelector )
                        .header( HttpHeader.AUTHORIZATION, "Bearer " + token )
                        .accept( MimeTypes.Type.APPLICATION_JSON.asString() )
                        .send();

                log.info( "Received from k8s api \n" + response.getContentAsString() );

                KubernetesType serviceList = objectMapper.readValue( response.getContent(), KubernetesType.class );

                Collection<AdvertisedSocketAddress> addresses = serviceList.handle( new Parser( portName, namespace ) );

                userLog.info( "Resolved %s from Kubernetes API at %s namespace %s labelSelector %s",
                        addresses, kubernetesAddress, namespace, labelSelector );

                if ( addresses.isEmpty() )
                {
                    log.error( "Resolved empty hosts from Kubernetes API at %s namespace %s labelSelector %s",
                            kubernetesAddress, namespace, labelSelector );
                }

                return addresses;
            }
            catch ( IOException e )
            {
                log.error( "Failed to parse result from Kubernetes API", e );
                return Collections.emptySet();
            }
            catch ( InterruptedException | ExecutionException | TimeoutException e )
            {
                log.error(
                        String.format( "Failed to resolve hosts from Kubernetes API at %s namespace %s labelSelector %s",
                                kubernetesAddress, namespace, labelSelector ),
                        e );
                return Collections.emptySet();
            }
        }
    }

    private static class Parser implements KubernetesType.Visitor<Collection<AdvertisedSocketAddress>>
    {
        private final String portName;
        private final String namespace;

        private Parser( String portName, String namespace )
        {
            this.portName = portName;
            this.namespace = namespace;
        }

        @Override
        public Collection<AdvertisedSocketAddress> visit( Status status )
        {
            String message = String.format( "Unable to contact Kubernetes API. Status: %s", status );
            throw new IllegalStateException( message );
        }

        @Override
        public Collection<AdvertisedSocketAddress> visit( ServiceList serviceList )
        {
            Stream<Pair<String,ServiceList.Service.ServiceSpec.ServicePort>> serviceNamePortStream = serviceList
                    .items()
                    .stream()
                    .filter( this::notDeleted )
                    .flatMap( this::extractServicePort );

            return serviceNamePortStream
                    .map( serviceNamePort -> new AdvertisedSocketAddress(
                            String.format( "%s.%s.svc.cluster.local", serviceNamePort.first(), namespace ),
                            serviceNamePort.other().port() ) )
                    .collect( Collectors.toSet() );
        }

        private boolean notDeleted( ServiceList.Service service )
        {
            return service.metadata().deletionTimestamp() == null;
        }

        private Stream<Pair<String,ServiceList.Service.ServiceSpec.ServicePort>> extractServicePort( ServiceList.Service service )
        {
            return service.spec()
                    .ports()
                    .stream()
                    .filter( port -> portName.equals( port.name() ) )
                    .map( port -> Pair.of( service.metadata().name(), port ) );
        }
    }
}
