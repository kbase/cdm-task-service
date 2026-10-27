"""
Registration and orchestration service for registered AI models.
"""

from collections.abc import Callable
import datetime
import logging

from cdmtaskservice.ai_models.huggingface_client import resolve_revision, ResolvedRevision
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO, Tag
from cdmtaskservice import sites
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSUser


class AIModelRegistry:
    """
    A manager for registered AI models.
    """

    def __init__(
        self,
        mongo: MongoDAO,
        coro_manager: CoroutineWrangler,
        flow_manager: JobFlowManager,
        _timestamp_fn: Callable[[], datetime.datetime] = utcdatetime,
    ):
        """
        Create the AI model registry.

        mongo - a MongoDB DAO.
        coro_manager - a coroutine manager.
        flow_manager - the job flow manager.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._flowman = _not_falsy(flow_manager, "flow_manager")
        self._timestamp_fn = _timestamp_fn

    async def register_ai_model(
        self,
        source: models.AIModelSource,
        model_ref: str,
        user: CTSUser,
    ) -> models.AdminAIModel:
        """
        Register an AI model snapshot, resolving its ref to a version and staging it at every
        usable cluster if it isn't already registered.

        source - the source of the AI model.
        model_ref - the combined model@ref reference, e.g. `org/name@main`. ref must be either
            a literal resolved version or a real ref that exists at the provider (e.g. an HF
            branch or git tag).
        user - the user registering the AI model.

        Returns the registered AI model.
        """
        _not_falsy(source, "source")
        model, ref = self._split_model_ref(model_ref)
        _not_falsy(user, "user")
        resolved = self._resolve_ref(source, model, ref)
        version = resolved.revision_hash
        tag = None if ref.lower() == version.lower() else ref
        clusters = await self._flowman.list_usable_clusters()
        if not clusters:
            raise ValueError("No job flows are available")
        now = self._timestamp_fn()
        ai_model = self._new_ai_model(
            source, model, version, resolved.size_bytes, user, clusters, now
        )
        save_tag = Tag(tag=tag, user=user.user, time=now) if tag else None
        res = await self._mongo.save_ai_model(ai_model, tag=save_tag)
        if not res.model_created:
            return await self._handle_already_registered(ai_model, tag, res.tag_moved, user)
        await self._stage_at_clusters(ai_model, clusters)
        ai_model.tags = [tag] if tag else []
        return ai_model

    async def _handle_already_registered(
        self,
        ai_model: models.AdminAIModel,
        tag: str | None,
        tag_moved: bool,
        user: CTSUser,
    ) -> models.AdminAIModel:
        if tag_moved:
            logging.getLogger(__name__).info(
                f"User {user.user} tagged {tag} to AI model {ai_model.key}",
                extra={logfields.USER: user.user, logfields.AI_MODEL_ID: str(ai_model.key)},
            )
        return await self._mongo.get_ai_model(
            ai_model.source, ai_model.model, ai_model.version, populate_tags=True, as_admin=True
        )

    async def _stage_at_clusters(
        self, ai_model: models.AdminAIModel, clusters: set[sites.Cluster]
    ):
        for c in clusters:
            flow = await self._flowman.get_flow(c)
            await self._coman.run_coroutine(flow.stage_ai_model(ai_model))

    @staticmethod
    def _split_model_ref(model_ref: str) -> tuple[str, str]:
        model_ref = _require_string(model_ref, "model_ref")
        try:
            return models.split_ai_model_ref(model_ref)
        except ValueError as e:
            raise IllegalParameterError(str(e)) from e

    @staticmethod
    def _resolve_ref(source: models.AIModelSource, model: str, ref: str) -> ResolvedRevision:
        match source:
            case models.AIModelSource.HUGGINGFACE:
                return resolve_revision(model, ref)
            case _:
                # Impossible to test but here for safety if new sources are added
                raise ValueError(f"Unsupported AI model source: {source}")

    @staticmethod
    def _new_ai_model(
        source: models.AIModelSource,
        model: str,
        version: str,
        size_bytes: int,
        user: CTSUser,
        clusters: set[sites.Cluster],
        time: datetime.datetime,
    ) -> models.AdminAIModel:
        statuses = [
            models.AdminStagingStatus(
                cluster=c,
                state=models.StagingState.CREATED,
                transition_times=[models.StagingStateTransition(
                    state=models.StagingState.CREATED, time=time
                )],
            )
            for c in clusters
        ]
        return models.AdminAIModel(
            source=source,
            model=model,
            version=version,
            size_bytes=size_bytes,
            registered_by=user.user,
            registered_on=time,
            statuses=statuses,
        )

    async def get_ai_models(self) -> list[models.AIModel]:
        """
        Get AI models in the system in no particular order. Returns at most 1000 records.
        """
        # pass through method, don't want the routes talking directly to mongo
        return await self._mongo.get_ai_models()

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
        model, ref = self._split_model_ref(model_ref)
        return await self._mongo.get_ai_model(
            source, model, ref, resolve_tag=resolve_tag, populate_tags=populate_tags,
            as_admin=as_admin,
        )
