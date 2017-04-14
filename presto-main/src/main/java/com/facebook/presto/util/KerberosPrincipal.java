/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KerberosPrincipal
{
    private static final String HOSTNAME_PLACEHOLDER = "_HOST";
    private static final char REALM_SEPARATOR = '@';
    private static final char PARTS_SEPARATOR = '/';
    private static final char ESCAPE_CHARACTER = '\\';

    private final String username;
    private final Optional<String> hostname;
    private final Optional<String> realm;

    public KerberosPrincipal(String userName, Optional<String> hostName, Optional<String> realm)
    {
        this.username = requireNonNull(userName, "username is null");
        this.hostname = requireNonNull(hostName, "hostname is null");
        this.realm = requireNonNull(realm, "realm is null");
    }

    public String getUserName()
    {
        return username;
    }

    public Optional<String> getHostName()
    {
        return hostname;
    }

    public Optional<String> getRealm()
    {
        return realm;
    }

    public KerberosPrincipal substituteHostnamePlaceholder()
    {
        return substituteHostnamePlaceholder(getLocalCanonicalHostName());
    }

    public KerberosPrincipal substituteHostnamePlaceholder(String hostname)
    {
        if (this.hostname.isPresent() && this.hostname.get().equals(HOSTNAME_PLACEHOLDER)) {
            return new KerberosPrincipal(username, Optional.of(hostname), realm);
        }
        return this;
    }

    private static String getLocalCanonicalHostName()
    {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(username);
        hostname.ifPresent(hostname -> builder.append(PARTS_SEPARATOR).append(hostname));
        realm.ifPresent(realm -> builder.append(REALM_SEPARATOR).append(realm));
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KerberosPrincipal that = (KerberosPrincipal) o;
        return Objects.equals(username, that.username) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(realm, that.realm);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username, hostname, realm);
    }

    public static KerberosPrincipal valueOf(String value)
    {
        Parser parser = new Parser(value);
        return new KerberosPrincipal(parser.getUsername(), parser.getHostname(), parser.getRealm());
    }

    private static class Parser
    {
        private final String value;
        private String username;
        private Optional<String> realm;
        private Optional<String> hostname;

        public Parser(String value)
        {
            this.value = requireNonNull(value, "value is null");
            parse();
        }

        public String getUsername()
        {
            return username;
        }

        public Optional<String> getRealm()
        {
            return realm;
        }

        public Optional<String> getHostname()
        {
            return hostname;
        }

        private void parse()
        {
            int hostnameSeparatorIndex = -1;
            int realmSeparatorIndex = -1;

            boolean escape = false;
            for (int index = 0; index < value.length(); index++) {
                switch (value.charAt(index)) {
                    case ESCAPE_CHARACTER:
                        escape = !escape;
                        break;
                    case PARTS_SEPARATOR:
                        if (escape) {
                            escape = false;
                        }
                        else {
                            hostnameSeparatorIndex = index;
                        }
                        break;
                    case REALM_SEPARATOR:
                        if (escape) {
                            escape = false;
                        }
                        else {
                            realmSeparatorIndex = index;
                        }
                        break;
                    default:
                        escape = false;
                }
            }

            if (hostnameSeparatorIndex >= 0 && realmSeparatorIndex >= 0 && hostnameSeparatorIndex > realmSeparatorIndex) {
                throw invalidKerberosPrincipal(value);
            }

            String usernameAndHostname;
            if (realmSeparatorIndex >= 0) {
                usernameAndHostname = value.substring(0, realmSeparatorIndex);
                String realmString = value.substring(realmSeparatorIndex + 1, value.length());
                if (usernameAndHostname.isEmpty() || realmString.isEmpty()) {
                    throw invalidKerberosPrincipal(value);
                }
                realm = Optional.of(realmString);
            }
            else {
                usernameAndHostname = value;
                realm = Optional.empty();
            }

            if (hostnameSeparatorIndex >= 0) {
                username = usernameAndHostname.substring(0, hostnameSeparatorIndex);
                String hostnameString = usernameAndHostname.substring(hostnameSeparatorIndex + 1, usernameAndHostname.length());
                if (username.isEmpty() || hostnameString.isEmpty()) {
                    throw invalidKerberosPrincipal(value);
                }
                hostname = Optional.of(hostnameString);
            }
            else {
                username = usernameAndHostname;
                hostname = Optional.empty();
            }
        }

        private static IllegalArgumentException invalidKerberosPrincipal(String value)
        {
            return new IllegalArgumentException("Invalid Kerberos principal: " + value);
        }
    }
}
