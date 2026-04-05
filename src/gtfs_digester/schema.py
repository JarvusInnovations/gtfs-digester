"""GTFS schema definitions: primary keys, column order, required columns, numeric sort columns.

Covers all files from the GTFS Schedule specification. Unknown files are handled
by the archive layer without a schema definition — they are preserved and
fingerprinted using lexicographic row sorting.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass(frozen=True)
class FileSchema:
    """Schema definition for a single GTFS file."""

    filename: str
    columns: list[str]
    primary_key: list[str]
    required_columns: list[str] = field(default_factory=list)
    numeric_sort_columns: dict[str, pa.DataType] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for col in self.primary_key:
            if col not in self.columns:
                raise ValueError(
                    f"Primary key column '{col}' not in columns for {self.filename}"
                )
        for col in self.required_columns:
            if col not in self.columns:
                raise ValueError(
                    f"Required column '{col}' not in columns for {self.filename}"
                )
        for col in self.numeric_sort_columns:
            if col not in self.columns:
                raise ValueError(
                    f"Numeric sort column '{col}' not in columns for {self.filename}"
                )


# ---------------------------------------------------------------------------
# Core GTFS file schemas
# ---------------------------------------------------------------------------

AGENCY = FileSchema(
    filename="agency.txt",
    columns=[
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
        "agency_fare_url",
        "agency_email",
    ],
    primary_key=["agency_id"],
    required_columns=["agency_name", "agency_url", "agency_timezone"],
)

STOPS = FileSchema(
    filename="stops.txt",
    columns=[
        "stop_id",
        "stop_code",
        "stop_name",
        "tts_stop_name",
        "stop_desc",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "stop_url",
        "location_type",
        "parent_station",
        "stop_timezone",
        "wheelchair_boarding",
        "level_id",
        "platform_code",
    ],
    primary_key=["stop_id"],
    required_columns=["stop_id"],
)

ROUTES = FileSchema(
    filename="routes.txt",
    columns=[
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_type",
        "route_url",
        "route_color",
        "route_text_color",
        "route_sort_order",
        "continuous_pickup",
        "continuous_drop_off",
        "network_id",
    ],
    primary_key=["route_id"],
    required_columns=["route_id", "route_type"],
)

TRIPS = FileSchema(
    filename="trips.txt",
    columns=[
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "trip_short_name",
        "direction_id",
        "block_id",
        "shape_id",
        "wheelchair_accessible",
        "bikes_allowed",
    ],
    primary_key=["trip_id"],
    required_columns=["route_id", "service_id", "trip_id"],
)

STOP_TIMES = FileSchema(
    filename="stop_times.txt",
    columns=[
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "stop_headsign",
        "pickup_type",
        "drop_off_type",
        "continuous_pickup",
        "continuous_drop_off",
        "shape_dist_traveled",
        "timepoint",
    ],
    primary_key=["trip_id", "stop_sequence"],
    required_columns=["trip_id", "stop_id", "stop_sequence"],
    numeric_sort_columns={"stop_sequence": pa.int32()},
)

CALENDAR = FileSchema(
    filename="calendar.txt",
    columns=[
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ],
    primary_key=["service_id"],
    required_columns=[
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ],
)

CALENDAR_DATES = FileSchema(
    filename="calendar_dates.txt",
    columns=[
        "service_id",
        "date",
        "exception_type",
    ],
    primary_key=["service_id", "date"],
    required_columns=["service_id", "date", "exception_type"],
)

SHAPES = FileSchema(
    filename="shapes.txt",
    columns=[
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
        "shape_dist_traveled",
    ],
    primary_key=["shape_id", "shape_pt_sequence"],
    required_columns=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
    numeric_sort_columns={"shape_pt_sequence": pa.int32()},
)

FEED_INFO = FileSchema(
    filename="feed_info.txt",
    columns=[
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "default_lang",
        "feed_start_date",
        "feed_end_date",
        "feed_version",
        "feed_contact_email",
        "feed_contact_url",
    ],
    primary_key=[],  # single row, no primary key
    required_columns=["feed_publisher_name", "feed_publisher_url", "feed_lang"],
)

# ---------------------------------------------------------------------------
# Additional GTFS files (Fares V2, Flex, etc.)
# ---------------------------------------------------------------------------

FARE_ATTRIBUTES = FileSchema(
    filename="fare_attributes.txt",
    columns=[
        "fare_id",
        "price",
        "currency_type",
        "payment_method",
        "transfers",
        "agency_id",
        "transfer_duration",
    ],
    primary_key=["fare_id"],
    required_columns=["fare_id", "price", "currency_type", "payment_method", "transfers"],
)

FARE_RULES = FileSchema(
    filename="fare_rules.txt",
    columns=[
        "fare_id",
        "route_id",
        "origin_id",
        "destination_id",
        "contains_id",
    ],
    primary_key=[],  # no unique key defined in spec
    required_columns=["fare_id"],
)

FREQUENCIES = FileSchema(
    filename="frequencies.txt",
    columns=[
        "trip_id",
        "start_time",
        "end_time",
        "headway_secs",
        "exact_times",
    ],
    primary_key=["trip_id", "start_time"],
    required_columns=["trip_id", "start_time", "end_time", "headway_secs"],
)

TRANSFERS = FileSchema(
    filename="transfers.txt",
    columns=[
        "from_stop_id",
        "to_stop_id",
        "from_route_id",
        "to_route_id",
        "from_trip_id",
        "to_trip_id",
        "transfer_type",
        "min_transfer_time",
    ],
    primary_key=["from_stop_id", "to_stop_id", "from_route_id", "to_route_id", "from_trip_id", "to_trip_id"],
    required_columns=["transfer_type"],
)

PATHWAYS = FileSchema(
    filename="pathways.txt",
    columns=[
        "pathway_id",
        "from_stop_id",
        "to_stop_id",
        "pathway_mode",
        "is_bidirectional",
        "length",
        "traversal_time",
        "stair_count",
        "max_slope",
        "min_width",
        "signposted_as",
        "reversed_signposted_as",
    ],
    primary_key=["pathway_id"],
    required_columns=["pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional"],
)

LEVELS = FileSchema(
    filename="levels.txt",
    columns=[
        "level_id",
        "level_index",
        "level_name",
    ],
    primary_key=["level_id"],
    required_columns=["level_id", "level_index"],
)

TRANSLATIONS = FileSchema(
    filename="translations.txt",
    columns=[
        "table_name",
        "field_name",
        "language",
        "translation",
        "record_id",
        "record_sub_id",
        "field_value",
    ],
    primary_key=["table_name", "field_name", "language", "record_id", "record_sub_id", "field_value"],
    required_columns=["table_name", "field_name", "language", "translation"],
)

ATTRIBUTIONS = FileSchema(
    filename="attributions.txt",
    columns=[
        "attribution_id",
        "agency_id",
        "route_id",
        "trip_id",
        "organization_name",
        "is_producer",
        "is_operator",
        "is_authority",
        "attribution_url",
        "attribution_email",
        "attribution_phone",
    ],
    primary_key=["attribution_id"],
    required_columns=["organization_name"],
)

# Fares V2
AREAS = FileSchema(
    filename="areas.txt",
    columns=["area_id", "area_name"],
    primary_key=["area_id"],
    required_columns=["area_id"],
)

STOP_AREAS = FileSchema(
    filename="stop_areas.txt",
    columns=["area_id", "stop_id"],
    primary_key=["area_id", "stop_id"],
    required_columns=["area_id", "stop_id"],
)

NETWORKS = FileSchema(
    filename="networks.txt",
    columns=["network_id", "network_name"],
    primary_key=["network_id"],
    required_columns=["network_id"],
)

ROUTE_NETWORKS = FileSchema(
    filename="route_networks.txt",
    columns=["network_id", "route_id"],
    primary_key=["network_id", "route_id"],
    required_columns=["network_id", "route_id"],
)

FARE_MEDIA = FileSchema(
    filename="fare_media.txt",
    columns=["fare_media_id", "fare_media_name", "fare_media_type"],
    primary_key=["fare_media_id"],
    required_columns=["fare_media_id", "fare_media_type"],
)

FARE_PRODUCTS = FileSchema(
    filename="fare_products.txt",
    columns=["fare_product_id", "fare_product_name", "fare_media_id", "amount", "currency"],
    primary_key=["fare_product_id", "fare_media_id"],
    required_columns=["fare_product_id", "amount", "currency"],
)

FARE_LEG_RULES = FileSchema(
    filename="fare_leg_rules.txt",
    columns=[
        "leg_group_id",
        "network_id",
        "from_area_id",
        "to_area_id",
        "from_timeframe_group_id",
        "to_timeframe_group_id",
        "fare_product_id",
        "rule_priority",
    ],
    primary_key=[],  # no unique key in spec
    required_columns=["fare_product_id"],
)

FARE_TRANSFER_RULES = FileSchema(
    filename="fare_transfer_rules.txt",
    columns=[
        "from_leg_group_id",
        "to_leg_group_id",
        "transfer_count",
        "duration_limit",
        "duration_limit_type",
        "fare_transfer_type",
        "fare_product_id",
    ],
    primary_key=[],  # no unique key in spec
    required_columns=["fare_transfer_type"],
)

TIMEFRAMES = FileSchema(
    filename="timeframes.txt",
    columns=["timeframe_group_id", "start_time", "end_time", "service_id"],
    primary_key=["timeframe_group_id", "start_time", "end_time", "service_id"],
    required_columns=["timeframe_group_id", "start_time", "end_time", "service_id"],
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_ALL_SCHEMAS = [
    AGENCY,
    STOPS,
    ROUTES,
    TRIPS,
    STOP_TIMES,
    CALENDAR,
    CALENDAR_DATES,
    SHAPES,
    FEED_INFO,
    FARE_ATTRIBUTES,
    FARE_RULES,
    FREQUENCIES,
    TRANSFERS,
    PATHWAYS,
    LEVELS,
    TRANSLATIONS,
    ATTRIBUTIONS,
    AREAS,
    STOP_AREAS,
    NETWORKS,
    ROUTE_NETWORKS,
    FARE_MEDIA,
    FARE_PRODUCTS,
    FARE_LEG_RULES,
    FARE_TRANSFER_RULES,
    TIMEFRAMES,
]

GTFS_SCHEMAS: dict[str, FileSchema] = {s.filename: s for s in _ALL_SCHEMAS}


def get_schema(filename: str) -> FileSchema | None:
    """Look up the schema for a GTFS filename. Returns None if unknown."""
    return GTFS_SCHEMAS.get(filename)


# Time columns that need zero-padding normalization
TIME_COLUMNS: frozenset[str] = frozenset({
    "arrival_time",
    "departure_time",
    "start_time",
    "end_time",
})
