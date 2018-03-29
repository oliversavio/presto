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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Result of fetching Partitions in the HivePartitionManager interface.
 * <p>
 * Results are comprised of two parts:
 * 1) The actual partitions
 * 2) The TupleDomain that represents the values that the connector was not able to pre-evaluate
 * when generating the partitions and will need to be double-checked by the final execution plan.
 */
public class HivePartitionResult
{
    private final List<HiveColumnHandle> partitionColumns;
    private final Iterable<HivePartition> partitions;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> unenforcedConstraint;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<HiveBucketHandle> bucketHandle;

    public HivePartitionResult(
            List<HiveColumnHandle> partitionColumns,
            Iterable<HivePartition> partitions,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> unenforcedConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle)
    {
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
    }

    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    public Iterator<HivePartition> getPartitions()
    {
        return partitions.iterator();
    }

    public List<HivePartition> getPartitionsAsList(int maxSize)
    {
        ImmutableList.Builder<HivePartition> partitionList = ImmutableList.builder();
        int count = 0;
        Iterator<HivePartition> iterator = partitions.iterator();
        while (iterator.hasNext()) {
            HivePartition partition = iterator.next();
            if (count == maxSize) {
                throw new PrestoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                        "Query over table '%s' can potentially read more than %s partitions",
                        partition.getTableName(),
                        maxSize));
            }
            partitionList.add(partition);
            count++;
        }
        return partitionList.build();
    }

    public TupleDomain<? extends ColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }

    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }
}
