resource ST_CleanupByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_cbc: ST_CleanupByCampaign) if
    has_role(user, "member", st_cbc.campaign);


resource ST_CleanupByTimeseries {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_cbt: ST_CleanupByTimeseries) if
    has_role(user, "member", st_cbt.timeseries.campaign_scope);
