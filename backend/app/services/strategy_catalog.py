"""Transactional catalog for immutable, content-addressed strategy versions."""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset_registry import AssetRegistry
from app.models.strategy import Strategy, StrategyVersion
from app.services.exceptions import StrategyDefinitionError, StrategyNotFoundError
from app.strategies.contracts import (
    CanonicalStrategyDefinition,
    InstrumentSnapshot,
    StrategyDefinitionInput,
    StrategyMetadata,
    StrategyVersionReference,
)


class StrategyCatalogService:
    """Creates stable strategy identities and immutable executable revisions.

    A duplicate definition for a strategy is idempotent: it returns the existing
    version rather than allocating another version number.  A changed canonical
    definition is serialized under a row lock on the parent strategy, ensuring
    version numbers are monotonically allocated even with multiple workers.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_strategy(self, metadata: StrategyMetadata) -> Strategy:
        async with self.db.begin():
            result = await self.db.execute(
                select(Strategy).where(Strategy.strategy_key == metadata.strategy_key).with_for_update()
            )
            if result.scalars().first() is not None:
                raise StrategyDefinitionError(f"strategy {metadata.strategy_key!r} already exists")

            strategy = Strategy(
                strategy_key=metadata.strategy_key,
                name=metadata.name,
                description=metadata.description,
            )
            try:
                # A row lock cannot protect a key for which no row exists yet.
                # Convert the concurrent first-create unique race into the
                # documented domain error rather than leaking IntegrityError.
                async with self.db.begin_nested():
                    self.db.add(strategy)
                    await self.db.flush()
            except IntegrityError:
                raced = await self.db.execute(
                    select(Strategy).where(Strategy.strategy_key == metadata.strategy_key).with_for_update()
                )
                if raced.scalars().first() is not None:
                    raise StrategyDefinitionError(f"strategy {metadata.strategy_key!r} already exists")
                raise
            return strategy

    async def create_version(
        self,
        strategy_key: str,
        definition: StrategyDefinitionInput,
    ) -> StrategyVersion:
        """Resolve asset snapshots and persist an immutable version atomically."""
        async with self.db.begin():
            strategy = await self._locked_strategy(strategy_key)
            assets = await self._locked_asset_snapshots(self._referenced_asset_ids(definition))
            canonical_definition = CanonicalStrategyDefinition.from_input(definition, assets)
            definition_hash = canonical_definition.definition_hash()

            existing_result = await self.db.execute(
                select(StrategyVersion)
                .where(
                    StrategyVersion.strategy_id == strategy.id,
                    StrategyVersion.definition_hash == definition_hash,
                )
                .with_for_update()
            )
            existing = existing_result.scalars().first()
            if existing is not None:
                return existing

            latest_result = await self.db.execute(
                select(func.max(StrategyVersion.version_number)).where(StrategyVersion.strategy_id == strategy.id)
            )
            latest_version = latest_result.scalar_one_or_none()
            next_version = (latest_version or 0) + 1

            version = StrategyVersion(
                strategy_id=strategy.id,
                version_number=next_version,
                schema_version=canonical_definition.schema_version,
                definition_hash=definition_hash,
                canonical_definition=canonical_definition.canonical_payload(),
            )
            self.db.add(version)
            await self.db.flush()
            return version

    async def get_canonical_definition(
        self,
        strategy_key: str,
        version_number: int,
    ) -> tuple[StrategyVersionReference, CanonicalStrategyDefinition]:
        """Load a version and verify its stored content still matches its digest."""
        result = await self.db.execute(
            select(Strategy, StrategyVersion)
            .join(StrategyVersion, StrategyVersion.strategy_id == Strategy.id)
            .where(
                Strategy.strategy_key == strategy_key,
                StrategyVersion.version_number == version_number,
            )
        )
        row = result.first()
        if row is None:
            raise StrategyNotFoundError(f"strategy version {strategy_key!r} v{version_number} not found")

        strategy, version = row
        try:
            canonical_definition = CanonicalStrategyDefinition.model_validate(version.canonical_definition)
        except ValueError as exc:
            raise StrategyDefinitionError("stored strategy definition is invalid") from exc
        actual_hash = canonical_definition.definition_hash()
        if actual_hash != version.definition_hash:
            raise StrategyDefinitionError("stored strategy definition does not match its definition hash")

        return (
            StrategyVersionReference(
                strategy_key=strategy.strategy_key,
                version_number=version.version_number,
                definition_hash=version.definition_hash,
            ),
            canonical_definition,
        )

    async def _locked_strategy(self, strategy_key: str) -> Strategy:
        result = await self.db.execute(
            select(Strategy).where(Strategy.strategy_key == strategy_key).with_for_update()
        )
        strategy = result.scalars().first()
        if strategy is None:
            raise StrategyNotFoundError(f"strategy {strategy_key!r} not found")
        return strategy

    async def _locked_asset_snapshots(
        self,
        asset_ids: Iterable[int],
    ) -> dict[int, InstrumentSnapshot]:
        required_asset_ids = set(asset_ids)
        result = await self.db.execute(
            select(AssetRegistry)
            .where(AssetRegistry.id.in_(required_asset_ids))
            .with_for_update()
        )
        assets = result.scalars().all()
        snapshots = {
            asset.id: InstrumentSnapshot(
                asset_id=asset.id,
                symbol=asset.symbol,
                exchange=asset.exchange,
                asset_type=asset.asset_type,
            )
            for asset in assets
        }
        missing = sorted(required_asset_ids.difference(snapshots))
        if missing:
            raise StrategyDefinitionError(f"strategy references unknown assets: {missing}")
        return snapshots

    @staticmethod
    def _referenced_asset_ids(definition: StrategyDefinitionInput) -> set[int]:
        asset_ids = set(definition.universe_asset_ids)
        if definition.benchmark_asset_id is not None:
            asset_ids.add(definition.benchmark_asset_id)
        return asset_ids
