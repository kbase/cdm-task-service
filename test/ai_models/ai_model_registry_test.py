import logging
import pytest
from unittest.mock import call, create_autospec, patch

from cdmtaskservice.ai_models.huggingface_client import AIModelResolutionError, ResolvedRevision
from cdmtaskservice.ai_models.registry import AIModelRegistry
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice.jobflows.flowmanager import JobFlow, JobFlowManager
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO, NoSuchAIModelError, SaveAIModelResult, Tag
from cdmtaskservice import sites
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSUser


_USER = CTSUser(user="aparkin")
_T = utcdatetime()


class _FakeCoroutineWrangler:
    """ Runs coroutines to completion immediately, same shape as kbase_test.py's fake. """

    async def run_coroutine(self, coro):
        await coro


def _make_registry(_timestamp_fn=None):
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    flowman = create_autospec(JobFlowManager, spec_set=True, instance=True)
    registry = AIModelRegistry(
        mongo,
        _FakeCoroutineWrangler(),
        flowman,
        _timestamp_fn=_timestamp_fn if _timestamp_fn is not None else lambda: _T,
    )
    return registry, mongo, flowman


def _resolve_revision_patch():
    return patch("cdmtaskservice.ai_models.registry.resolve_revision")


def _with_sorted_statuses(ai_model: models.AdminAIModel) -> models.AdminAIModel:
    return ai_model.model_copy(
        update={"statuses": sorted(ai_model.statuses, key=lambda s: s.cluster.value)}
    )


def _expected_single_cluster_ai_model(
    version: str, tags: list[str] | None = None
) -> models.AdminAIModel:
    return models.AdminAIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version=version,
        size_bytes=42,
        registered_by=_USER.user,
        registered_on=_T,
        statuses=[models.AdminStagingStatus(
            cluster=sites.Cluster.PERLMUTTER_JAWS,
            state=models.StagingState.CREATED,
            transition_times=[models.StagingStateTransition(
                state=models.StagingState.CREATED, time=_T
            )],
        )],
        tags=tags,
    )


def test_constructor_bad_args():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    coman = create_autospec(CoroutineWrangler, spec_set=True, instance=True)
    flowman = create_autospec(JobFlowManager, spec_set=True, instance=True)
    with pytest.raises(ValueError, match="^mongo is required$"):
        AIModelRegistry(None, coman, flowman)
    with pytest.raises(ValueError, match="^coro_manager is required$"):
        AIModelRegistry(mongo, None, flowman)
    with pytest.raises(ValueError, match="^flow_manager is required$"):
        AIModelRegistry(mongo, coman, None)


# model_ref is parsed identically by register_ai_model and get_ai_model, both via
# AIModelRegistry._split_model_ref - so each bad-ref case is exercised against both methods.


@pytest.mark.parametrize("bad_model_ref", [None, "", "   "])
async def test_empty_model_ref_rejected_by_both_methods(bad_model_ref):
    registry, _, _ = _make_registry()
    with pytest.raises(ValueError, match="^model_ref is required$"):
        await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, bad_model_ref, _USER
        )
    with pytest.raises(ValueError, match="^model_ref is required$"):
        await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, bad_model_ref)


@pytest.mark.parametrize("model_ref", ["org/name", "org/name@", "@main"])
async def test_malformed_model_ref_rejected_by_both_methods(model_ref):
    registry, _, _ = _make_registry()
    match = f"Invalid AI model reference '{model_ref}'; expected the form model@ref"
    with pytest.raises(IllegalParameterError, match=match):
        await registry.register_ai_model(models.AIModelSource.HUGGINGFACE, model_ref, _USER)
    with pytest.raises(IllegalParameterError, match=match):
        await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, model_ref)


async def test_register_ai_model_bad_source():
    registry, _, _ = _make_registry()
    with pytest.raises(ValueError, match="^source is required$"):
        await registry.register_ai_model(None, "org/name@main", _USER)


async def test_register_ai_model_bad_user():
    registry, _, _ = _make_registry()
    with pytest.raises(ValueError, match="^user is required$"):
        await registry.register_ai_model(models.AIModelSource.HUGGINGFACE, "org/name@main", None)


async def test_register_ai_model_resolution_fails():
    registry, mongo, flowman = _make_registry()
    with _resolve_revision_patch() as resolve:
        resolve.side_effect = AIModelResolutionError("repo not found")
        with pytest.raises(AIModelResolutionError, match="^repo not found$"):
            await registry.register_ai_model(
                models.AIModelSource.HUGGINGFACE, "org/name@main", _USER
            )
    resolve.assert_called_once_with("org/name", "main")
    mongo.save_ai_model.assert_not_called()
    flowman.list_usable_clusters.assert_not_called()
    flowman.get_flow.assert_not_called()


async def test_register_ai_model_no_usable_clusters():
    registry, mongo, flowman = _make_registry()
    flowman.list_usable_clusters.return_value = set()
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="abc123", size_bytes=12345)
        with pytest.raises(ValueError, match="^No job flows are available$"):
            await registry.register_ai_model(
                models.AIModelSource.HUGGINGFACE, "org/name@main", _USER
            )
    mongo.save_ai_model.assert_not_called()
    flowman.get_flow.assert_not_called()


async def test_register_ai_model_new_registration_from_hash():
    # ref is already the resolved hash (case-insensitively) - no tag is derived.
    registry, mongo, flowman = _make_registry()
    flowman.list_usable_clusters.return_value = {sites.Cluster.PERLMUTTER_JAWS}
    flow = create_autospec(JobFlow, spec_set=True, instance=True)
    flowman.get_flow.return_value = flow
    mongo.save_ai_model.return_value = SaveAIModelResult(model_created=True, tag_moved=False)
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="ABC123", size_bytes=42)
        res = await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, "org/name@abc123", _USER
        )
    resolve.assert_called_once_with("org/name", "abc123")
    expected = _expected_single_cluster_ai_model("ABC123", tags=[])
    assert res == expected
    mongo.save_ai_model.assert_called_once_with(expected, tag=None)
    flowman.get_flow.assert_called_once_with(sites.Cluster.PERLMUTTER_JAWS)
    flow.stage_ai_model.assert_awaited_once_with(res)


async def test_register_ai_model_new_registration_two_clusters():
    # Staging is fanned out to every usable cluster, not just one.
    registry, mongo, flowman = _make_registry()
    clusters = [sites.Cluster.PERLMUTTER_JAWS, sites.Cluster.KBASE]
    flowman.list_usable_clusters.return_value = set(clusters)
    flows = {c: create_autospec(JobFlow, spec_set=True, instance=True) for c in clusters}
    flowman.get_flow.side_effect = lambda cluster: flows[cluster]
    mongo.save_ai_model.return_value = SaveAIModelResult(model_created=True, tag_moved=False)
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="ABC123", size_bytes=42)
        res = await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, "org/name@abc123", _USER
        )
    resolve.assert_called_once_with("org/name", "abc123")
    expected = models.AdminAIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="ABC123",
        size_bytes=42,
        registered_by=_USER.user,
        registered_on=_T,
        statuses=[
            models.AdminStagingStatus(
                cluster=c,
                state=models.StagingState.CREATED,
                transition_times=[models.StagingStateTransition(
                    state=models.StagingState.CREATED, time=_T
                )],
            )
            for c in clusters
        ],
        tags=[],
    )
    mongo.save_ai_model.assert_called_once_with(res, tag=None)
    # get_flow is called once per cluster from a set, so call order isn't guaranteed - assert
    # the exact set of calls regardless of order.
    flowman.get_flow.assert_has_calls([call(c) for c in clusters], any_order=True)
    assert flowman.get_flow.call_count == len(clusters)
    for c in clusters:
        flows[c].stage_ai_model.assert_awaited_once_with(res)
    # statuses order follows set iteration order, which isn't guaranteed - compare with
    # statuses sorted by cluster on both sides so the assertion doesn't depend on that order.
    assert _with_sorted_statuses(res) == _with_sorted_statuses(expected)


async def test_register_ai_model_new_registration_from_tag():
    # ref differs from the resolved hash - ref itself becomes the derived tag.
    registry, mongo, flowman = _make_registry()
    flowman.list_usable_clusters.return_value = {sites.Cluster.PERLMUTTER_JAWS}
    flow = create_autospec(JobFlow, spec_set=True, instance=True)
    flowman.get_flow.return_value = flow
    mongo.save_ai_model.return_value = SaveAIModelResult(model_created=True, tag_moved=True)
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="abc123", size_bytes=42)
        res = await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, "org/name@main", _USER
        )
    resolve.assert_called_once_with("org/name", "main")
    expected = _expected_single_cluster_ai_model("abc123", tags=["main"])
    assert res == expected
    mongo.save_ai_model.assert_called_once_with(
        expected, tag=Tag(tag="main", user=_USER.user, time=_T)
    )
    flow.stage_ai_model.assert_awaited_once_with(res)


async def test_register_ai_model_already_registered_no_tag():
    registry, mongo, flowman = _make_registry()
    flowman.list_usable_clusters.return_value = {sites.Cluster.PERLMUTTER_JAWS}
    mongo.save_ai_model.return_value = SaveAIModelResult(model_created=False, tag_moved=False)
    expected = models.AdminAIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="abc123", size_bytes=42)
        res = await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, "org/name@abc123", _USER
        )
    assert res == expected
    # ref ("abc123") already equals the resolved version, so no tag is derived and save_ai_model
    # is still called unconditionally (no separate existence check before it).
    mongo.save_ai_model.assert_called_once_with(
        _expected_single_cluster_ai_model("abc123"), tag=None
    )
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "abc123",
        populate_tags=True, as_admin=True,
    )
    flowman.get_flow.assert_not_called()


async def test_register_ai_model_already_registered_tag_moved(caplog):
    caplog.set_level(logging.INFO)
    registry, mongo, flowman = _make_registry()
    flowman.list_usable_clusters.return_value = {sites.Cluster.PERLMUTTER_JAWS}
    mongo.save_ai_model.return_value = SaveAIModelResult(model_created=False, tag_moved=True)
    expected = models.AdminAIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    with _resolve_revision_patch() as resolve:
        resolve.return_value = ResolvedRevision(revision_hash="abc123", size_bytes=42)
        res = await registry.register_ai_model(
            models.AIModelSource.HUGGINGFACE, "org/name@main", _USER
        )
    assert res == expected
    # ref ("main") differs from the resolved version, so it's derived as the tag to save, even
    # though the model itself is already registered (save_ai_model is still called
    # unconditionally, and is the only place that can move an existing tag's pointer).
    mongo.save_ai_model.assert_called_once_with(
        _expected_single_cluster_ai_model("abc123"),
        tag=Tag(tag="main", user=_USER.user, time=_T),
    )
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "abc123",
        populate_tags=True, as_admin=True,
    )
    flowman.get_flow.assert_not_called()
    assert "aparkin tagged main to AI model huggingface/org/name/abc123" in caplog.text


async def test_get_ai_model_bad_source():
    registry, _, _ = _make_registry()
    with pytest.raises(ValueError, match="^source is required$"):
        await registry.get_ai_model(None, "org/name@main")


async def test_get_ai_model_ref_contains_slash():
    registry, mongo, _ = _make_registry()
    expected = models.AIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    res = await registry.get_ai_model(
        models.AIModelSource.HUGGINGFACE, "org/name@releases/v1.0", resolve_tag=True,
    )
    assert res == expected
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "releases/v1.0",
        resolve_tag=True, populate_tags=False, as_admin=False,
    )


async def test_get_ai_model_splits_on_last_at():
    # The ref itself contains an "@" - splitting on the first "@" would wrongly take "user" as
    # the ref and leave a dangling "@releases/v1.0" off the model.
    registry, mongo, _ = _make_registry()
    expected = models.AIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    res = await registry.get_ai_model(
        models.AIModelSource.HUGGINGFACE, "org/name@user@releases/v1.0", resolve_tag=True,
    )
    assert res == expected
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name@user", "releases/v1.0",
        resolve_tag=True, populate_tags=False, as_admin=False,
    )


async def test_get_ai_model_default_args():
    registry, mongo, _ = _make_registry()
    expected = models.AIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    res = await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, "org/name@main")
    assert res == expected
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "main",
        resolve_tag=False, populate_tags=False, as_admin=False,
    )


async def test_get_ai_model_non_default_args():
    registry, mongo, _ = _make_registry()
    expected = models.AdminAIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    res = await registry.get_ai_model(
        models.AIModelSource.HUGGINGFACE, "org/name@main",
        resolve_tag=True, populate_tags=True, as_admin=True,
    )
    assert res == expected
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "main",
        resolve_tag=True, populate_tags=True, as_admin=True,
    )


async def test_recover_ai_model():
    registry, mongo, flowman = _make_registry()
    key = models.AIModelKey(models.AIModelSource.HUGGINGFACE, "org/name", "abc123")
    existing_status = models.AdminStagingStatus(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        state=models.StagingState.ERROR,
        transition_times=[models.StagingStateTransition(
            state=models.StagingState.ERROR, time=_T
        )],
        error="boom",
    )
    ai_model = models.AdminAIModel(
        source=key.source,
        model=key.model,
        version=key.version,
        size_bytes=42,
        registered_by=_USER.user,
        registered_on=_T,
        statuses=[existing_status],
    )
    mongo.get_ai_model.return_value = ai_model
    flow = create_autospec(JobFlow, spec_set=True, instance=True)
    flowman.get_flow.return_value = flow

    await registry.recover_ai_model(key, sites.Cluster.KBASE)

    mongo.get_ai_model.assert_called_once_with(
        key.source, key.model, key.version, as_admin=True
    )
    expected_new_status = models.AdminStagingStatus(
        cluster=sites.Cluster.KBASE,
        state=models.StagingState.CREATED,
        transition_times=[models.StagingStateTransition(
            state=models.StagingState.CREATED, time=_T
        )],
    )
    flowman.get_flow.assert_called_once_with(sites.Cluster.KBASE)
    mongo.add_ai_model_site.assert_called_once_with(key, expected_new_status)
    # Built independently of `ai_model` (rather than reusing that same, possibly mutated,
    # reference) so this actually verifies the content staged, not just object identity.
    expected_staged_ai_model = models.AdminAIModel(
        source=key.source,
        model=key.model,
        version=key.version,
        size_bytes=42,
        registered_by=_USER.user,
        registered_on=_T,
        statuses=[existing_status, expected_new_status],
    )
    flow.stage_ai_model.assert_awaited_once_with(expected_staged_ai_model)


async def test_recover_ai_model_fail_inputs():
    registry, _, _ = _make_registry()
    key = models.AIModelKey(models.AIModelSource.HUGGINGFACE, "org/name", "abc123")
    with pytest.raises(ValueError, match="^key is required$"):
        await registry.recover_ai_model(None, sites.Cluster.KBASE)
    with pytest.raises(ValueError, match="^cluster is required$"):
        await registry.recover_ai_model(key, None)


async def test_recover_ai_model_fail_no_such_model():
    registry, mongo, flowman = _make_registry()
    key = models.AIModelKey(models.AIModelSource.HUGGINGFACE, "org/name", "abc123")
    mongo.get_ai_model.side_effect = NoSuchAIModelError(f"No AI model with key '{key}' exists")

    with pytest.raises(NoSuchAIModelError, match=f"^No AI model with key '{key}' exists$"):
        await registry.recover_ai_model(key, sites.Cluster.KBASE)
    flowman.get_flow.assert_not_called()
    mongo.add_ai_model_site.assert_not_called()


async def test_recover_ai_model_fail_no_usable_flow():
    registry, mongo, flowman = _make_registry()
    key = models.AIModelKey(models.AIModelSource.HUGGINGFACE, "org/name", "abc123")
    ai_model = models.AdminAIModel(
        source=key.source,
        model=key.model,
        version=key.version,
        size_bytes=42,
        registered_by=_USER.user,
        registered_on=_T,
        statuses=[],
    )
    mongo.get_ai_model.return_value = ai_model
    flowman.get_flow.side_effect = ValueError("no flow available for cluster kbase")

    with pytest.raises(ValueError, match="^no flow available for cluster kbase$"):
        await registry.recover_ai_model(key, sites.Cluster.KBASE)
    mongo.add_ai_model_site.assert_not_called()


async def test_get_ai_models():
    registry, mongo, _ = _make_registry()
    expected = [models.AIModel.model_construct(), models.AIModel.model_construct()]
    mongo.get_ai_models.return_value = expected
    res = await registry.get_ai_models()
    assert res == expected
    mongo.get_ai_models.assert_called_once_with()
