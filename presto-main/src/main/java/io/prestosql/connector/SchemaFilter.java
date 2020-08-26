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
package io.prestosql.connector;

import io.airlift.slice.Slice;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;

import java.util.Objects;
import java.util.Optional;

public class SchemaFilter
{
    private final Optional<String> schemaName;
    private final Optional<String> schemaPrefix;

    public SchemaFilter(Optional<String> schemaName, Optional<String> schemaPrefix)
    {
        this.schemaName = schemaName;
        this.schemaPrefix = schemaPrefix;
    }

    public static SchemaFilter empty()
    {
        return new SchemaFilter(Optional.empty(), Optional.empty());
    }

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public Optional<String> getSchemaPattern()
    {
        if (schemaName.isPresent()) {
            return schemaName;
        }
        return schemaPrefix.map(prefix -> prefix + ".*");
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, schemaPrefix);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SchemaFilter other = (SchemaFilter) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.schemaPrefix, other.schemaPrefix);
    }

    @Override
    public String toString()
    {
        return schemaName.orElse("*") + '.' + schemaPrefix.orElse("*");
    }

    public static <T> Optional<String> prefixFilterString(TupleDomain<T> constraint, T column)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return Optional.empty();
        }

        if (domain.getValues() instanceof SortedRangeSet) {
            ValueSet valueSet = domain.getValues();
            if (!valueSet.isNone() && valueSet.getRanges().getRangeCount() == 1) {
                Range span = valueSet.getRanges().getSpan();
                return Optional.of(((Slice) span.getLow().getValue()).toStringUtf8());
            }
        }
        return Optional.empty();
    }
}
