package io.ssaownership.schemasync.model;

/** Desired state, owned by the deployment/chart. */
public class SchemaBindingSpec {
    private String schemaVersion;
    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }
}
