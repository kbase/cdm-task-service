"""
Registration and orchestration service for registered AI models.
"""

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO


class AIModelRegistry:
    """
    A manager for registered AI models.
    """

    def __init__(self, mongo: MongoDAO):
        """
        Create the AI model registry.

        mongo - a MongoDB DAO.
        """
        self._mongo = _not_falsy(mongo, "mongo")

    async def get_ai_model(
        self,
        source: models.AIModelSource,
        model_ref: str,
        resolve_tag: bool = False,
        populate_tags: bool = False,
        as_admin: bool = False,
    ) -> models.AIModel | models.AdminAIModel:
        """
        Get an AI model given a combined model@ref reference.

        source - the source of the AI model.
        model_ref - the combined model@ref reference, e.g. `org/name@main`.
        resolve_tag - if the given ref doesn't match a version exactly, fall back to resolving
            it as a tag.
        populate_tags - also look up and populate the tags field on the returned AI model.
        as_admin - get additional details about the AI model.
        """
        _not_falsy(source, "source")
        model, ref = _split_model_ref(model_ref)
        return await self._mongo.get_ai_model(
            source, model, ref, resolve_tag=resolve_tag, populate_tags=populate_tags,
            as_admin=as_admin,
        )


def _split_model_ref(model_ref: str) -> tuple[str, str]:
    model_ref = _require_string(model_ref, "model_ref")
    model, sep, ref = model_ref.rpartition("@")
    if not sep or not model or not ref:
        raise IllegalParameterError(
            f"Invalid AI model reference '{model_ref}'; expected the form model@ref"
        )
    return model, ref
