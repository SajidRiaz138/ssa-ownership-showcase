package io.ssaownership.schemasync.model;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * {@code SchemaBinding} custom resource.
 * <ul>
 *   <li>{@code spec.schemaVersion} — DESIRED state, owned by the deployment/Helm chart.</li>
 *   <li>{@code status} — OBSERVED state, owned by the operator (via the /status subresource).</li>
 * </ul>
 * The whole point of the showcase: an operator must record what it observed in {@code status},
 * NOT contend for {@code spec.schemaVersion}, which the chart owns.
 */
@Group ("ssaownership.io")
@Version ("v1")
public class SchemaBinding extends CustomResource<SchemaBindingSpec, SchemaBindingStatus> implements Namespaced
{
}
