package io.ssaownership.schemasync.model;

/** Observed state, written by the operator through the /status subresource (a normal status
 *  update, not server-side apply — so "written by", not SSA-"owned by"). */
public class SchemaBindingStatus {
    private String observedSchemaVersion;
    private String phase;
    public String getObservedSchemaVersion() { return observedSchemaVersion; }
    public void setObservedSchemaVersion(String v) { this.observedSchemaVersion = v; }
    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
}
