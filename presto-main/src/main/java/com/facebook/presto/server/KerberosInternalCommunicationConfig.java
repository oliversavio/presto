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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;

public class KerberosInternalCommunicationConfig
{
    private String kerberosPrincipal;
    private String kerberosServiceName;
    private File kerberosKeytab;
    private File kerberosConfig;
    private boolean kerberosUseCanonicalHostname = true;
    private File kerberosCredentialCache;

    @NotNull
    public String getKerberosPrincipal()
    {
        return kerberosPrincipal;
    }

    @Config("internal-communication.authentication.krb5.principal")
    public KerberosInternalCommunicationConfig setKerberosPrincipal(String kerberosPrincipal)
    {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    @NotNull
    public String getKerberosServiceName()
    {
        return kerberosServiceName;
    }

    @Config("internal-communication.authentication.krb5.service-name")
    public KerberosInternalCommunicationConfig setKerberosServiceName(String kerberosServiceName)
    {
        this.kerberosServiceName = kerberosServiceName;
        return this;
    }

    @NotNull
    public File getKerberosKeytab()
    {
        return kerberosKeytab;
    }

    @Config("internal-communication.authentication.krb5.keytab")
    public KerberosInternalCommunicationConfig setKerberosKeytab(File kerberosKeytab)
    {
        this.kerberosKeytab = kerberosKeytab;
        return this;
    }

    @NotNull
    public File getKerberosConfig()
    {
        return kerberosConfig;
    }

    @Config("internal-communication.authentication.krb5.config")
    public KerberosInternalCommunicationConfig setKerberosConfig(File kerberosConfig)
    {
        this.kerberosConfig = kerberosConfig;
        return this;
    }

    public boolean isKerberosUseCanonicalHostname()
    {
        return kerberosUseCanonicalHostname;
    }

    @Config("internal-communication.authentication.krb5.use-canonical-hostname")
    public KerberosInternalCommunicationConfig setKerberosUseCanonicalHostname(boolean kerberosUseCanonicalHostname)
    {
        this.kerberosUseCanonicalHostname = kerberosUseCanonicalHostname;
        return this;
    }

    public File getKerberosCredentialCache()
    {
        return kerberosCredentialCache;
    }

    @Config("internal-communication.authentication.krb5.credential-cache")
    public KerberosInternalCommunicationConfig setKerberosCredentialCache(File kerberosCredentialCache)
    {
        this.kerberosCredentialCache = kerberosCredentialCache;
        return this;
    }
}
