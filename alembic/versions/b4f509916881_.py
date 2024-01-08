"""empty message

Revision ID: b4f509916881
Revises: 236ace2b5fcf
Create Date: 2024-01-08 13:15:24.070418

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b4f509916881"
down_revision: Union[str, None] = "236ace2b5fcf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "tokenpos", sa.Column("ttype", sa.Integer(), nullable=False, server_default="1")
    )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("tokenpos", "ttype")
    # ### end Alembic commands ###