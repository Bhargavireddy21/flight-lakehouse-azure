"""Single place for table names and paths. Everything hangs off the Unity Catalog catalog name."""
from dataclasses import dataclass


@dataclass(frozen=True)
class LakeConfig:
    catalog: str = "flightlake"

    # Volumes (files)
    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/landing/raw"

    @property
    def checkpoint_path(self) -> str:
        return f"/Volumes/{self.catalog}/meta/checkpoints"

    # Batch tables
    @property
    def bronze_flights(self) -> str:
        return f"{self.catalog}.bronze.flights_raw"

    @property
    def silver_flights(self) -> str:
        return f"{self.catalog}.silver.flights"

    @property
    def silver_quarantine(self) -> str:
        return f"{self.catalog}.silver.flights_quarantine"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.gold.{table}"

    # Streaming tables
    @property
    def bronze_events(self) -> str:
        return f"{self.catalog}.bronze.flight_events_raw"

    @property
    def silver_events(self) -> str:
        return f"{self.catalog}.silver.flight_events"
