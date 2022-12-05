resource User {
    permissions = ["create", "read", "update", "delete", "set_admin", "set_active"];
    roles = ["self"];

    "read" if "self";
    "update" if "self";
}

has_role(_user: User{id: id}, "self", _user: User{id: id});


resource UserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["member"];

    "read" if "member";
}

has_role(user: User, "member", ug: UserGroup) if
    ubug in user.users_by_user_groups and
    ubug.user_group = ug;


resource UserByUserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["owner"];

    "read" if "owner";
}

has_role(user: User, "owner", ubug: UserByUserGroup) if
    user = ubug.user;


resource Campaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["member"];

    "read" if "member";
}

has_role(user: User, "member", campaign: Campaign) if
    ugbc in campaign.user_groups_by_campaigns and
    ubug in user.users_by_user_groups and
    ubug.user_group = ugbc.user_group;


resource UserGroupByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["owner"];

    "read" if "owner";
}

has_role(user: User, "owner", ugbc: UserGroupByCampaign) if
    has_role(user, "member", ugbc.user_group);


resource CampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["member"];

    "read" if "member";
}

has_role(user: User, "member", cs: CampaignScope) if
    ugbcs in cs.user_groups_by_campaign_scopes and
    ubug in user.users_by_user_groups and
    ubug.user_group = ugbcs.user_group;


resource UserGroupByCampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["owner"];

    "read" if "owner";
}

has_role(user: User, "owner", ugbcs: UserGroupByCampaignScope) if
    has_role(user, "member", ugbcs.user_group);


resource TimeseriesProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource TimeseriesDataState{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource Timeseries {
    permissions = ["create", "read", "update", "delete", "read_data", "write_data"];
    relations = {
        campaign_scope: CampaignScope
    };

    "read" if "member" on "campaign_scope";
    "read_data" if "member" on "campaign_scope";
    "write_data" if "member" on "campaign_scope";
}

has_relation(cs: CampaignScope, "campaign_scope", ts: Timeseries) if
    cs = ts.campaign_scope;


resource TimeseriesPropertyData {
    permissions = ["create", "read", "update", "delete"];
    relations = {
        timeseries: Timeseries
    };

    "read" if "read" on "timeseries";
}

has_relation(ts: Timeseries, "timeseries", tspd: TimeseriesPropertyData) if
    ts = tspd.timeseries;


resource TimeseriesByDataState {
    permissions = ["create", "read", "update", "delete"];
    relations = {
        timeseries: Timeseries
    };

    "create" if "read" on "timeseries";
    "read" if "read" on "timeseries";
    "update" if "read" on "timeseries";
    "delete" if "read" on "timeseries";
}

has_relation(ts: Timeseries, "timeseries", tsbds: TimeseriesByDataState) if
    ts = tsbds.timeseries;


resource EventCategory{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource EventLevel{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource Event {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: User, "create", event:Event) if
    has_role(user, "member", event.campaign_scope);
has_permission(user: User, "read", event:Event) if
    has_role(user, "member", event.campaign_scope);
has_permission(user: User, "update", event:Event) if
    has_role(user, "member", event.campaign_scope);
has_permission(user: User, "delete", event:Event) if
    has_role(user, "member", event.campaign_scope);


resource TimeseriesByEvent {
    permissions = ["create", "read", "update", "delete"];
}

# TODO: Oso issue: using campaign scope twice in permission triggers an error
# in Oso when building the query:
# "Type `CampaignScope` occurs more than once as the target of a relation"
# Let's check only event and ensure in code that timeseries is in the same
# campaign scope.
# When/if fixed in Oso, check that TS/event relations are refreshed before
# Oso performs the test.

has_permission(user: User, "create", tbe:TimeseriesByEvent) if
#    tbe.timeseries.campaign_scope = tbe.event.campaign_scope and
    has_permission(user, "update", tbe.event);
has_permission(user: User, "read", tbe:TimeseriesByEvent) if
#    tbe.timeseries.campaign_scope = tbe.event.campaign_scope and
    has_permission(user, "read", tbe.event);
has_permission(user: User, "update", tbe:TimeseriesByEvent) if
#    tbe.timeseries.campaign_scope = tbe.event.campaign_scope and
    has_permission(user, "update", tbe.event);
has_permission(user: User, "delete", tbe:TimeseriesByEvent) if
#    tbe.timeseries.campaign_scope = tbe.event.campaign_scope and
    has_permission(user, "update", tbe.event);


resource StructuralElementProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource SiteProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource BuildingProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource StoreyProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource SpaceProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource ZoneProperty{
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}


resource Site {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", site:Site) if
    has_role(user, "member", site.campaign);

resource Building {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", building:Building) if
    has_permission(user, "read", building.site);

resource Storey {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", storey:Storey) if
    has_permission(user, "read", storey.building);

resource Space{
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", space:Space) if
    has_permission(user, "read", space.storey);

resource Zone {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", zone:Zone) if
    has_role(user, "member", zone.campaign);


# TODO: Oso issue: checking both site and timeseries involves user x group
# twice and triggers an error in Oso when building the query:
# "Type `UserGroup` occurs more than once as the target of a relation"
# Fortunately, in our case, checking only timeseries is enough because users
# having access to the timeseries "should" be part of the campaign and
# therefore should have access to the site.

resource TimeseriesBySite {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", tbs:TimeseriesBySite) if
    #has_permission(user, "read", tbs.site) and
    has_permission(user, "read", tbs.timeseries);

resource TimeseriesByBuilding {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", tbb:TimeseriesByBuilding) if
    #has_permission(user, "read", tbb.building) and
    has_permission(user, "read", tbb.timeseries);

resource TimeseriesByStorey {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", tbs:TimeseriesByStorey) if
    #has_permission(user, "read", tbs.storey) and
    has_permission(user, "read", tbs.timeseries);

resource TimeseriesBySpace {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", tbs:TimeseriesBySpace) if
    #has_permission(user, "read", tbs.space) and
    has_permission(user, "read", tbs.timeseries);

resource TimeseriesByZone {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", tbz:TimeseriesByZone) if
    #has_permission(user, "read", tbz.zone) and
    has_permission(user, "read", tbz.timeseries);


resource SitePropertyData {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", spd:SitePropertyData) if
    has_permission(user, "read", spd.site);

resource BuildingPropertyData {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", bpd:BuildingPropertyData) if
    has_permission(user, "read", bpd.building);

resource StoreyPropertyData {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", spd:StoreyPropertyData) if
    has_permission(user, "read", spd.storey);

resource SpacePropertyData {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", spd:SpacePropertyData) if
    has_permission(user, "read", spd.space);

resource ZonePropertyData {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", zpd:ZonePropertyData) if
    has_permission(user, "read", zpd.zone);


resource EnergySource {
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}

resource EnergyEndUse {
    permissions = ["create", "read", "update", "delete"];
    roles = ["user"];

    "read" if "user";
}

resource EnergyConsumptionTimeseriesBySite {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", ecbs: EnergyConsumptionTimeseriesBySite) if
    has_permission(user, "read_data", ecbs.timeseries);

resource EnergyConsumptionTimeseriesByBuilding {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", ecbb: EnergyConsumptionTimeseriesByBuilding) if
    has_permission(user, "read_data", ecbb.timeseries);
