"""cagg_refresh_lease

Revision ID: 007_cagg_refresh_lease
Revises: 006_export_jobs
Create Date: 2026-08-11 22:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '007_cagg_refresh_lease'
down_revision: Union[str, None] = '006_export_jobs'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('cagg_refresh_jobs', sa.Column('worker_id', sa.String(), nullable=True))
    op.add_column('cagg_refresh_jobs', sa.Column('claimed_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('cagg_refresh_jobs', sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True))

def downgrade() -> None:
    op.drop_column('cagg_refresh_jobs', 'lease_expires_at')
    op.drop_column('cagg_refresh_jobs', 'claimed_at')
    op.drop_column('cagg_refresh_jobs', 'worker_id')
