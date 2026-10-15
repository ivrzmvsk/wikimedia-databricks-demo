"""Sync managed AI Search; wait for a *new* Lakeflow update, never old ready=true."""
import time


def sync_and_wait(client, index_name, source_table, embedding_endpoint, *, timeout=1200,
                  sleep=time.sleep, clock=time.monotonic):
    deadline = clock() + timeout
    index = client.vector_search_indexes.get_index(index_name)
    spec = index.delta_sync_index_spec
    if (not spec or spec.source_table != source_table or spec.pipeline_type.value != "TRIGGERED"
            or index.primary_key != "chunk_id" or len(spec.embedding_source_columns or []) != 1
            or spec.embedding_source_columns[0].name != "text"
            or spec.embedding_source_columns[0].embedding_model_endpoint_name != embedding_endpoint):
        raise RuntimeError("Existing index configuration differs; use a reviewed new index for model/schema migration")
    pipeline = spec.pipeline_id
    if not pipeline:
        raise RuntimeError("AI Search did not expose its sync pipeline ID")
    before = client.pipelines.list_updates(pipeline_id=pipeline).updates or []
    if any(u.state.value not in ("COMPLETED", "FAILED", "CANCELED") for u in before):
        raise RuntimeError("A sync is already active; retry after it finishes")
    previous = {u.update_id for u in before}
    client.vector_search_indexes.sync_index(index_name)
    update_id = None
    while clock() < deadline:
        updates = client.pipelines.list_updates(pipeline_id=pipeline).updates or []
        # Managed pipelines can automatically retry a failed API_CALL. Those
        # retries are not concurrent external sync requests; track the original.
        fresh = [u for u in updates if u.update_id not in previous
                 and getattr(getattr(u, "cause", None), "value", None) != "RETRY_ON_FAILURE"]
        if len(fresh) > 1:
            raise RuntimeError("Concurrent external sync observed; readiness is ambiguous")
        if fresh:
            update_id = fresh[0].update_id
            state = client.pipelines.get_update(pipeline_id=pipeline, update_id=update_id).update.state.value
            if state in ("FAILED", "CANCELED"):
                raise RuntimeError(f"AI Search sync {update_id}: {state}")
            if state == "COMPLETED":
                current = client.vector_search_indexes.get_index(index_name)
                if current.status and current.status.ready:
                    return dict(update_id=update_id, indexed_row_count=current.status.indexed_row_count)
        sleep(min(5, max(0, deadline - clock())))
    raise TimeoutError(f"New AI Search sync did not complete: {update_id}")
