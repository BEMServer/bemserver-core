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


resource ST_CheckMissingByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_cbc: ST_CheckMissingByCampaign) if
    has_role(user, "member", st_cbc.campaign);


resource ST_CheckOutliersByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_cbc: ST_CheckOutliersByCampaign) if
    has_role(user, "member", st_cbc.campaign);


resource ST_DownloadWeatherDataBySite {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_dwdbs: ST_DownloadWeatherDataBySite) if
    has_role(user, "member", st_dwdbs.site.campaign);


resource ST_DownloadWeatherForecastDataBySite {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "read", st_dwdbs: ST_DownloadWeatherForecastDataBySite) if
    has_role(user, "member", st_dwdbs.site.campaign);
