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

package com.facebook.presto.connector.system;

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MetadataBasedSystemTablesProvider
        implements SystemTablesProvider
{
    private final Metadata metadata;
    private final String catalogName;

    public MetadataBasedSystemTablesProvider(Metadata metadata, String catalogName)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return metadata.getSystemTable(
                ((FullConnectorSession) session).getSession(),
                new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName()));
    }
}
