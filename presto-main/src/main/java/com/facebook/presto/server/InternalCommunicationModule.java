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
package com.facebook.presto.server;

import com.facebook.presto.util.KerberosPrincipal;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.spnego.KerberosConfig;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class InternalCommunicationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, config -> {
            config.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
            config.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
        });

        install(installModuleIf(InternalCommunicationConfig.class, InternalCommunicationConfig::isKerberosEnabled, kerberosInternalCommunicationModule()));
    }

    private Module kerberosInternalCommunicationModule()
    {
        return binder -> {
            KerberosInternalCommunicationConfig internalKerberosConfig = buildConfigObject(KerberosInternalCommunicationConfig.class);
            com.facebook.presto.server.security.KerberosConfig serverKerberosConfig = buildConfigObject(com.facebook.presto.server.security.KerberosConfig.class);

            checkState(serverKerberosConfig.getKeytab() != null && serverKerberosConfig.getKeytab().getAbsolutePath().equals(internalKerberosConfig.getKerberosKeytab().getAbsolutePath()), "kerberos keytab values must match");
            checkState(serverKerberosConfig.getServiceName() != null && serverKerberosConfig.getServiceName().equals(internalKerberosConfig.getKerberosServiceName()), "kerberos servicename values must match");
            checkState(serverKerberosConfig.getKerberosConfig() != null && serverKerberosConfig.getKerberosConfig().getAbsolutePath().equals(internalKerberosConfig.getKerberosConfig().getAbsolutePath()), "kerberos keytab values must match");

            configBinder(binder).bindConfigGlobalDefaults(KerberosConfig.class, kerberosConfig -> {
                kerberosConfig.setConfig(internalKerberosConfig.getKerberosConfig());
                kerberosConfig.setKeytab(internalKerberosConfig.getKerberosKeytab());
                kerberosConfig.setUseCanonicalHostname(internalKerberosConfig.isKerberosUseCanonicalHostname());
                kerberosConfig.setCredentialCache(internalKerberosConfig.getKerberosCredentialCache());
            });

            KerberosPrincipal principal = KerberosPrincipal.valueOf(internalKerberosConfig.getKerberosPrincipal());
            configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, httpClientConfig -> {
                httpClientConfig.setAuthenticationEnabled(true);
                httpClientConfig.setKerberosPrincipal(principal.substituteHostnamePlaceholder().toString());
                httpClientConfig.setKerberosRemoteServiceName(internalKerberosConfig.getKerberosServiceName());
            });
        };
    }
}
