import pytest
from unittest.mock import create_autospec

from cdmtaskservice.ai_models.registry import AIModelRegistry
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO


def _make_registry():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    return AIModelRegistry(mongo), mongo


def test_constructor_bad_args():
    with pytest.raises(ValueError, match="^mongo is required$"):
        AIModelRegistry(None)


async def test_get_ai_model_bad_source():
    registry, _ = _make_registry()
    with pytest.raises(ValueError, match="^source is required$"):
        await registry.get_ai_model(None, "org/name@main")


@pytest.mark.parametrize("bad_model_ref", [None, "", "   "])
async def test_get_ai_model_bad_model_ref(bad_model_ref):
    registry, _ = _make_registry()
    with pytest.raises(ValueError, match="^model_ref is required$"):
        await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, bad_model_ref)


@pytest.mark.parametrize("model_ref", ["org/name", "org/name@", "@main"])
async def test_get_ai_model_malformed_ref(model_ref):
    registry, _ = _make_registry()
    with pytest.raises(
        IllegalParameterError,
        match=f"Invalid AI model reference '{model_ref}'; expected the form model@ref",
    ):
        await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, model_ref)


async def test_get_ai_model_ref_contains_slash():
    registry, mongo = _make_registry()
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
    registry, mongo = _make_registry()
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
    registry, mongo = _make_registry()
    expected = models.AIModel.model_construct()
    mongo.get_ai_model.return_value = expected
    res = await registry.get_ai_model(models.AIModelSource.HUGGINGFACE, "org/name@main")
    assert res == expected
    mongo.get_ai_model.assert_called_once_with(
        models.AIModelSource.HUGGINGFACE, "org/name", "main",
        resolve_tag=False, populate_tags=False, as_admin=False,
    )


async def test_get_ai_model_non_default_args():
    registry, mongo = _make_registry()
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
