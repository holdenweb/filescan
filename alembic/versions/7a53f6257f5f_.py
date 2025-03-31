"""empty message

Revision ID: 7a53f6257f5f
Revises: b4f509916881
Create Date: 2025-03-31 16:25:51.668905

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7a53f6257f5f"
down_revision: Union[str, None] = "b4f509916881"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE location ALTER COLUMN filesize TYPE bigint USING filesize")


def downgrade() -> None:
    op.execute("ALTER TABLE location ALTER COLUMN filesize TYPE int USING filesize")
