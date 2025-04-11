-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH FlatEvents AS (
    -- Flatten the main event-level data
    SELECT
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        user_pseudo_id,
        event_timestamp,
        event_name,
        event_params,
        user_properties,
        items
    FROM
        `your_project_id.your_dataset_id.events_*` -- Replace with your actual project and dataset
    WHERE 
        _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
),

FlatEventParams AS (
    -- Unnest event parameters
    SELECT
        user_pseudo_id,
        event_timestamp,
        event_name,
        ep.key AS param_key,
        ep.value.string_value AS param_string_value,
        ep.value.int_value AS param_int_value,
        ep.value.float_value AS param_float_value,
        ep.value.double_value AS param_double_value
    FROM
        FlatEvents, UNNEST(event_params) AS ep
),

FlatUserProperties AS (
    -- Unnest user properties
    SELECT
        user_pseudo_id,
        event_timestamp,
        event_name,
        up.key AS user_property_key,
        up.value.string_value AS user_property_string_value,
        up.value.int_value AS user_property_int_value,
        up.value.float_value AS user_property_float_value,
        up.value.double_value AS user_property_double_value,
        up.value.set_timestamp_micros AS user_property_set_timestamp
    FROM
        FlatEvents, UNNEST(user_properties) AS up
),

FlatItems AS (
    -- Unnest item-level data
    SELECT
        user_pseudo_id,
        event_timestamp,
        event_name,
        item.item_id,
        item.item_name,
        item.item_brand,
        item.item_variant,
        item.item_category,
        item.item_category2,
        item.item_category3,
        item.item_category4,
        item.item_category5,
        item.price_in_usd,
        item.price,
        item.quantity,
        item.item_revenue_in_usd,
        item.item_revenue,
        item.item_refund_in_usd,
        item.item_refund,
        item.coupon,
        item.affiliation,
        item.location_id,
        item.item_list_id,
        item.item_list_name,
        item.item_list_index,
        item.promotion_id,
        item.promotion_name,
        item.creative_name,
        item.creative_slot
    FROM
        FlatEvents, UNNEST(items) AS item
)

SELECT
    fe.user_pseudo_id,
    fe.event_timestamp,
    fe.event_name,
    -- Event Parameters
    fep.param_key,
    fep.param_string_value,
    fep.param_int_value,
    fep.param_float_value,
    fep.param_double_value,
    -- User Properties
    fup.user_property_key,
    fup.user_property_string_value,
    fup.user_property_int_value,
    fup.user_property_float_value,
    fup.user_property_double_value,
    fup.user_property_set_timestamp,
    -- Item Data
    fi.item_id,
    fi.item_name,
    fi.item_brand,
    fi.item_variant,
    fi.item_category,
    fi.item_category2,
    fi.item_category3,
    fi.item_category4,
    fi.item_category5,
    fi.price_in_usd,
    fi.price,
    fi.quantity,
    fi.item_revenue_in_usd,
    fi.item_revenue,
    fi.item_refund_in_usd,
    fi.item_refund,
    fi.coupon,
    fi.affiliation,
    fi.location_id,
    fi.item_list_id,
    fi.item_list_name,
    fi.item_list_index,
    fi.promotion_id,
    fi.promotion_name,
    fi.creative_name,
    fi.creative_slot
FROM 
    FlatEvents fe
LEFT JOIN 
    FlatEventParams fep 
    ON fe.user_pseudo_id = fep.user_pseudo_id 
    AND fe.event_timestamp = fep.event_timestamp 
    AND fe.event_name = fep.event_name
LEFT JOIN 
    FlatUserProperties fup 
    ON fe.user_pseudo_id = fup.user_pseudo_id 
    AND fe.event_timestamp = fup.event_timestamp 
    AND fe.event_name = fup.event_name
LEFT JOIN 
    FlatItems fi 
    ON fe.user_pseudo_id = fi.user_pseudo_id 
    AND fe.event_timestamp = fi.event_timestamp 
    AND fe.event_name = fi.event_name
ORDER BY 
    fe.user_pseudo_id, fe.event_timestamp;
