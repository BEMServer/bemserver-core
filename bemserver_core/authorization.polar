# TODO: Investigate potential Oso issue
# We should't have to write "*_member" or "*_owner, Oso should infer which
# has_role rule matches which resource type.

# General rule
allow(actor, action, resource) if has_permission(actor, action, resource);

# Open bar mode
allow(_actor, _action, _resource) if OpenBarPolarClass.get();

# Admin can do anything
allow(user: UserActor, _action, _resource) if user.is_admin = true;

# User has role "user" on anything
resource Base {
    roles = ["user"];
}

has_role(_: UserActor, "user", _: Base);


actor UserActor {}


resource User {
    permissions = ["create", "read", "update", "delete", "set_admin", "set_active"];
    roles = ["self"];

    "read" if "self";
    "update" if "self";
}

has_role(_user: UserActor{id: id}, "self", _user: User{id: id});


resource UserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ug_member"];

    "read" if "ug_member";
}

has_role(user: UserActor, "ug_member", ug: UserGroup) if
    ubug in ug.users_by_user_groups and
    ubug.user = user;


resource UserByUserGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ubug_owner"];

    "read" if "ubug_owner";
}

has_role(user: UserActor, "ubug_owner", ubug: UserByUserGroup) if
    user = ubug.user;


resource Campaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["c_member"];

    "read" if "c_member";
}

has_role(user: UserActor, "c_member", campaign: Campaign) if
    ugbc in campaign.user_groups_by_campaigns and
    ubug in ugbc.user_group.users_by_user_groups and
    user = ubug.user;


resource UserGroupByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ugbc_owner"];

    "read" if "ugbc_owner";
}

has_role(user: UserActor, "ugbc_owner", ugbc: UserGroupByCampaign) if
    has_role(user, "ug_member", ugbc.user_group);


resource CampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["cs_member"];

    "read" if "cs_member";
}

has_role(user: UserActor, "cs_member", cs: CampaignScope) if
    ugbcs in cs.user_groups_by_campaign_scopes and
    ubug in ugbcs.user_group.users_by_user_groups and
    user = ubug.user;


resource UserGroupByCampaignScope {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ugbcs_owner"];

    "read" if "ugbcs_owner";
}

has_role(user: UserActor, "ugbcs_owner", ugbcs: UserGroupByCampaignScope) if
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

has_permission(user: UserActor, "create", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: UserActor, "read", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: UserActor, "update", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);
has_permission(user: UserActor, "delete", event:Event) if
    has_role(user, "cs_member", event.campaign_scope);

