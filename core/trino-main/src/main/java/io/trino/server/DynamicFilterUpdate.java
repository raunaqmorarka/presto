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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;

public class DynamicFilterUpdate
{
    private final TupleDomain<ColumnHandle> currentPredicate;
    private final boolean isComplete;

    @JsonCreator
    public DynamicFilterUpdate(
            @JsonProperty("currentPredicate") TupleDomain<ColumnHandle> currentPredicate,
            @JsonProperty("isComplete") boolean isComplete)
    {
        this.currentPredicate = requireNonNull(currentPredicate, "currentPredicate is null");
        this.isComplete = isComplete;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        return currentPredicate;
    }

    @JsonProperty
    public boolean isComplete()
    {
        return isComplete;
    }
}
