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
package io.trino.plugin.hive;

import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HivePartitionedBucketFunction
        implements BucketFunction
{
    private final int numHiveBuckets;
    private final Optional<HiveBucketFunction> hiveBucketFunction;
    private final List<Type> partitionColumnTypes;
    private final List<List<NullableValue>> partitions;

    public HivePartitionedBucketFunction(HiveBucketing.BucketingVersion bucketingVersion, int bucketCount, List<HiveType> hiveTypes, List<Type> partitionColumnTypes, List<String> partitions)
    {
        if (bucketCount > 1) {
            this.numHiveBuckets = bucketCount / partitions.size();
        }
        else {
            this.numHiveBuckets = 1;
        }
        if (this.numHiveBuckets > 1) {
            this.hiveBucketFunction = Optional.of(new HiveBucketFunction(bucketingVersion, numHiveBuckets, hiveTypes));
        }
        else {
            this.hiveBucketFunction = Optional.empty();
        }
        this.partitionColumnTypes = requireNonNull(partitionColumnTypes, "partitionColumnTypes is null");
        this.partitions = getPartitionValues(requireNonNull(partitions, "partitions is null"), partitionColumnTypes);
    }

    @Override
    public int getBucket(Page page, int position)
    {
        int bucket = hiveBucketFunction.map(bucketFunction -> bucketFunction.getBucket(page, position)).orElse(0);
        int partition = 0;
        ImmutableList.Builder<NullableValue> partitionValueBuilder = ImmutableList.builder();
        for (int i = 0; i < partitionColumnTypes.size(); i++) {
            Block block = page.getBlock(i);
            Type type = partitionColumnTypes.get(i);
            NullableValue value;
            if (block.isNull(position)) {
                value = NullableValue.asNull(type);
            }
            else {
                value = HiveUtil.partitionKeyFromBlock(type, block, position);
            }
            partitionValueBuilder.add(value);
        }
        // TODO: don't do a linear search!
        List<NullableValue> partitionValue = partitionValueBuilder.build();
        for (int i = 0; i < partitions.size(); i++) {
            if (partitions.get(i).equals(partitionValue)) {
                partition = i;
                break;
            }
        }
        return bucket + (partition * numHiveBuckets);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("hiveBucketFunction", hiveBucketFunction)
                .add("partitions", partitions)
                .add("partitionColumnTypes", partitionColumnTypes);
        return helper.toString();
    }

    private static List<List<NullableValue>> getPartitionValues(List<String> partitions, List<Type> partitionColumnTypes)
    {
        return partitions.stream().map(partitionName -> {
            List<String> values = HivePartitionManager.extractPartitionValues(partitionName);
            ImmutableList.Builder<NullableValue> builder = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                NullableValue parsedValue = HiveUtil.parsePartitionValue(partitionName, values.get(i), partitionColumnTypes.get(i));
                builder.add(parsedValue);
            }
            return builder.build();
        }).collect(toImmutableList());
    }
}
