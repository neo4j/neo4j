/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration.ssl;

import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.GroupSettingHelper;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.string.SecureString;

@ServiceProvider
@PublicApi
public class SslPolicyConfig implements GroupSetting {
    public final Setting<Boolean> enabled;

    @Description("The mandatory base directory for cryptographic objects of this policy."
            + " It is also possible to override each individual configuration with absolute paths.")
    public final Setting<Path> base_directory;

    @Description("Path to directory of CRLs (Certificate Revocation Lists) in PEM format.")
    public final Setting<Path> revoked_dir;

    @Description("Makes this policy trust all remote parties."
            + " Enabling this is not recommended and the trusted directory will be ignored.")
    public final Setting<Boolean> trust_all;

    @Description("Makes this policy trust expired certificates."
            + " Enabling will also allow the instance to use an expired certificate itself.")
    public final Setting<Boolean> trust_expired;

    @Description("Client authentication stance.")
    public final Setting<ClientAuth> client_auth;

    @Description("Restrict allowed TLS protocol versions.")
    public final Setting<List<String>> tls_versions;

    @Description(
            "Restrict allowed ciphers. " + "Valid values depend on JRE and SSL however some examples can be found here "
                    + "https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#jsse-cipher-suite-names")
    public final Setting<List<String>> ciphers;

    @Description(
            "When true, this node will verify the hostname of every other instance it connects to by comparing the address it used to connect with it "
                    + "and the patterns described in the remote hosts public certificate Subject Alternative Names")
    public final Setting<Boolean> verify_hostname;

    @Description("Private PKCS#8 key in PEM format.")
    public final Setting<Path> private_key;

    @Description("The passphrase for the private key.")
    public final Setting<SecureString> private_key_password;

    @Description("X.509 certificate (chain) of this server in PEM format.")
    public final Setting<Path> public_certificate;

    @Description("Path to directory of X.509 certificates in PEM format for trusted parties.")
    public final Setting<Path> trusted_dir;

    private final SslPolicyScope scope;

    public static SslPolicyConfig forScope(SslPolicyScope scope) {
        return new SslPolicyConfig(scope.name());
    }

    private SslPolicyConfig(String scopeString) {
        scope = SslPolicyScope.fromName(scopeString);
        if (scope == null) {
            throw new IllegalArgumentException("SslPolicy can not be created for scope: " + scopeString);
        }

        enabled = getBuilder("enabled", BOOL, Boolean.FALSE).build();
        base_directory = getBuilder("base_directory", PATH, Path.of(scope.baseDir))
                .setDependency(neo4j_home)
                .immutable()
                .build();
        revoked_dir = getBuilder("revoked_dir", PATH, Path.of("revoked"))
                .setDependency(base_directory)
                .build();
        trust_all = getBuilder("trust_all", BOOL, false).build();
        trust_expired = getBuilder("trust_expired", BOOL, true).build();
        client_auth = getBuilder("client_auth", ofEnum(ClientAuth.class), scope.authDefault)
                .build();
        tls_versions = getBuilder("tls_versions", listOf(STRING), List.of("TLSv1.2", "TLSv1.3"))
                .build();
        ciphers = getBuilder("ciphers", listOf(STRING), null).build();
        verify_hostname = getBuilder("verify_hostname", BOOL, false).build();
        private_key = getBuilder("private_key", PATH, Path.of("private.key"))
                .setDependency(base_directory)
                .build();
        private_key_password =
                getBuilder("private_key_password", SECURE_STRING, null).build();
        public_certificate = getBuilder("public_certificate", PATH, Path.of("public.crt"))
                .setDependency(base_directory)
                .build();
        trusted_dir = getBuilder("trusted_dir", PATH, Path.of("trusted"))
                .setDependency(base_directory)
                .build();
    }

    public SslPolicyConfig() // For service loading
            {
        this("testing");
    }

    @Override
    public String name() {
        return scope.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String getPrefix() {
        return "dbms.ssl.policy";
    }

    public SslPolicyScope getScope() {
        return scope;
    }

    private <T> SettingBuilder<T> getBuilder(String suffix, SettingValueParser<T> parser, T defaultValue) {
        return GroupSettingHelper.getBuilder(getPrefix(), name(), suffix, parser, defaultValue);
    }
}
