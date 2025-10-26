from sqlalchemy import Column, DateTime, Integer, String

from .base_class import BaseTable

"""
'ORIGIN', 'DESTINATION', 'ORIGIN_PORT_CODE', 'DESTINATION_PORT_CODE',
'SERVICE_VERSION_AND_ROUNDTRIP_IDENTFIERS',
'ORIGIN_SERVICE_VERSION_AND_MASTER',
'DESTINATION_SERVICE_VERSION_AND_MASTER', 'ORIGIN_AT_UTC',
'OFFERED_CAPACITY_TEU'
"""


class SailingTable(BaseTable):
    __tablename__ = "sailings"

    origin = Column("origin", String(255), nullable=False)
    destination = Column("destination", String(255), nullable=False)
    origin_port_code = Column("origin_port_code", String(255), nullable=False)
    destination_port_code = Column("destination_port_code", String(255), nullable=False)
    service_version_and_roundtrip_identfiers = Column(
        "service_version_and_roundtrip_identfiers", String(255), nullable=False
    )
    origin_service_version_and_master = Column(
        "origin_service_version_and_master", String(255), nullable=False
    )
    destination_service_version_and_master = Column(
        "destination_service_version_and_master", String(255), nullable=False
    )
    origin_at_utc = Column("origin_at_utc", DateTime(timezone=True), nullable=False)
    offered_capacity_teu = Column("offered_capacity_teu", Integer, nullable=False)

    # TODO check which indexes are needed based on query patterns

    def __repr__(self):
        return (
            f"<SailingData(id={self.id}, "
            f"service='{self.service_version_and_roundtrip_identfiers}', "
            f"origin_at_utc='{self.origin_at_utc}')>"
        )
