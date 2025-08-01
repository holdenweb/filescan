"""Add relationship between RunLog and Archive

Revision ID: d9e23a3d2ca2
Revises: 7a53f6257f5f
Create Date: 2025-04-04 20:41:03.056524

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d9e23a3d2ca2"
down_revision: Union[str, None] = "7a53f6257f5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("archive", sa.Column("runlog_id", sa.Integer(), nullable=True))
    op.create_index(
        op.f("ix_archive_runlog_id"), "archive", ["runlog_id"], unique=False
    )
    op.create_foreign_key(
        op.f("fk_archive_runlog_id_runlog"), "archive", "runlog", ["runlog_id"], ["id"]
    )
    op.add_column("runlog", sa.Column("when_finished", sa.DateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("runlog", "when_finished")
    op.drop_constraint(
        op.f("fk_archive_runlog_id_runlog"), "archive", type_="foreignkey"
    )
    op.drop_index(op.f("ix_archive_runlog_id"), table_name="archive")
    op.drop_column("archive", "runlog_id")
    # ### end Alembic commands ###
