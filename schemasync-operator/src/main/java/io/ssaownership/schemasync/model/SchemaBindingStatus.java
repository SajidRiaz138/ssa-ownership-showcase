package io.ssaownership.schemasync.model;

/** Observed state, owned by the operator (written via the /status subresource). */
public class SchemaBindingStatus {
    private String observedSchemaVersion;
    private String phase;
    public String getObservedSchemaVersion() { return observedSchemaVersion; }
    public void setObservedSchemaVersion(String v) { this.observedSchemaVersion = v; }
    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
}
