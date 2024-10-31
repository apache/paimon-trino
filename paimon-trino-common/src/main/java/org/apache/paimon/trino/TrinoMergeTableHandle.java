package org.apache.paimon.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

/** Trino {@link ConnectorMergeTableHandle}. */
public class TrinoMergeTableHandle implements ConnectorMergeTableHandle {

    private final TrinoTableHandle tableHandle;

    @JsonCreator
    public TrinoMergeTableHandle(@JsonProperty("tableHandle") TrinoTableHandle tableHandle) {
        this.tableHandle = tableHandle;
    }

    @Override
    @JsonProperty
    public ConnectorTableHandle getTableHandle() {
        return tableHandle;
    }
}
