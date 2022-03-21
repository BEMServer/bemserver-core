# TODO: Investigate potential Oso issue
# We should't have to write "*_member" or "*_owner, Oso should infer which
# has_role rule matches which resource type.

# General rule
allow(actor, action, resource) if has_permission(actor, action, resource);

# Open bar mode
allow(_actor, _action, _resource) if OpenBarPolarClass.get();

# Admin can do anything
allow(user: User, _action, _resource) if user.is_admin = true;

# User has role "user" on anything
resource Base {
    roles = ["user"];
}

has_role(_: User, "user", _: Base);


actor User {}


resource User {
    permissions = ["create", "read", "update", "delete", "set_admin", "set_active"];
    roles = ["self"];

    "read" if "self";
    "update" if "self";
}

has_role(_user: User{id: id}, "self", _user: User{id: id});


resource UserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ug_member"];

    "read" if "ug_member";
}

has_role(user: User, "ug_member", ug: UserGroup) if
    ubug in user.users_by_user_groups and
    ubug.user_group = ug;


resource UserByUserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ubug_owner"];

    "read" if "ubug_owner";
}

has_role(user: User, "ubug_owner", ubug: UserByUserGroup) if
    user = ubug.user;


resource Campaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["c_member"];

    "read" if "c_member";
}

has_role(user: User, "c_member", campaign: Campaign) if
    ugbc in campaign.user_groups_by_campaigns and
    ubug in user.users_by_user_groups and
    ubug.user_group = ugbc.user_group;


resource UserGroupByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ugbc_owner"];

    "read" if "ugbc_owner";
}

has_role(user: User, "ugbc_owner", ugbc: UserGroupByCampaign) if
    has_role(user, "ug_member", ugbc.user_group);


resource CampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["cs_member"];

    "read" if "cs_member";
}

has_role(user: User, "cs_member", cs: CampaignScope) if
    ugbcs in cs.user_groups_by_campaign_scopes and
    ubug in user.users_by_user_groups and
    ubug.user_group = ugbcs.user_group;


resource UserGroupByCampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ugbcs_owner"];

    "read" if "ugbcs_owner";
}

has_role(user: User, "ugbcs_owner", ugbcs: UserGroupByCampaignScope) if
    has_role(user, "ug_member", ugbcs.user_group);


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

    "read" if "cs_member" on "campaign_scope";
    "read_data" if "cs_member" on "campaign_scope";
    "write_data" if "cs_member" on "campaign_scope";
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


resource EventState{
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
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: User, "read", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: User, "update", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: User, "delete", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);


resource Site {
    permissions = ["create", "read", "update", "delete"];
}
has_permission(user: User, "read", site:Site) if
    has_role(user, "c_member", site.campaign);

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
    has_role(user, "c_member", zone.campaign);
