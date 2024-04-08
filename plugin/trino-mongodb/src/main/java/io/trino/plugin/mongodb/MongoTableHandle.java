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
package io.trino.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;

import java.util.*;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MongoTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final RemoteTableName remoteTableName;
    private final Optional<String> filter;
    private final TupleDomain<ColumnHandle> constraint;
    private final Set<MongoColumnHandle> projectedColumns;
    private final OptionalInt limit;

    private final List<SortItem> sortItems;

    public MongoTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, Optional<String> filter)
    {
        this(schemaTableName, remoteTableName, filter, TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty(), List.of());
    }

    @JsonCreator
    public MongoTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("filter") Optional<String> filter,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Set<MongoColumnHandle> projectedColumns,
            @JsonProperty("limit") OptionalInt limit)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.filter = requireNonNull(filter, "filter is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.limit = requireNonNull(limit, "limit is null");
        this.sortItems = List.of();
    }

    @JsonCreator
    public MongoTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("filter") Optional<String> filter,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Set<MongoColumnHandle> projectedColumns,
            @JsonProperty("limit") OptionalInt limit,
            @JsonProperty("sortItems") List<SortItem> sortItems)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.filter = requireNonNull(filter, "filter is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.limit = requireNonNull(limit, "limit is null");
        this.sortItems = List.copyOf(requireNonNull(sortItems, "sortItems is null"));
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public Optional<String> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Set<MongoColumnHandle> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public OptionalInt getLimit()
    {
        return limit;
    }

    @JsonProperty
    public List<SortItem> getSortItems() { return sortItems; }

    public MongoTableHandle withProjectedColumns(Set<MongoColumnHandle> projectedColumns)
    {
        return new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                filter,
                constraint,
                projectedColumns,
                limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, filter, constraint, projectedColumns, limit);
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
        MongoTableHandle other = (MongoTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.remoteTableName, other.remoteTableName) &&
                Objects.equals(this.filter, other.filter) &&
                Objects.equals(this.constraint, other.constraint) &&
                Objects.equals(this.projectedColumns, other.projectedColumns) &&
                Objects.equals(this.limit, other.limit) &&
                Objects.equals(this.sortItems, other.sortItems);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("remoteTableName", remoteTableName)
                .add("filter", filter)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .add("limit", limit)
                .add("sortItems", sortItems)
                .toString();
    }
}
