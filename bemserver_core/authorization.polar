# TODO: Investigate potential Oso issue
# We should't have to write "c_member" and "tsg_member", Oso should infer which
# has_role rule matches which resource type.
# Same for *_owner.

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


resource Campaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["c_member"];

    "read" if "c_member";
}

has_role(user: UserActor, "c_member", campaign: Campaign) if
    ubc in campaign.users_by_campaigns and
    user = ubc.user;


resource UserByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ubc_owner"];

    "read" if "ubc_owner";
}

has_role(user: UserActor, "ubc_owner", ubc: UserByCampaign) if
    user = ubc.user;


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


resource TimeseriesGroupByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: UserActor, "read", tgbc: TimeseriesGroupByCampaign) if
    has_role(user, "c_member", tgbc.campaign) and
    has_role(user, "tsg_member", tgbc.timeseries_group);


resource TimeseriesGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["tsg_member"];

    "read" if "tsg_member";
}

has_role(user: UserActor, "tsg_member", tg: TimeseriesGroup) if
    tsgbu in tg.timeseries_groups_by_users and
    user = tsgbu.user;


resource TimeseriesGroupByUser {
    permissions = ["create", "read", "update", "delete"];
    roles = ["tsgbu_owner"];

    "read" if "tsgbu_owner";
}

has_role(user: UserActor, "tsgbu_owner", tsgbu: TimeseriesGroupByUser) if
    user = tsgbu.user;


resource Timeseries {
    permissions = ["create", "read", "update", "delete", "read_data", "write_data"];
    relations = {
        group: TimeseriesGroup
    };

    "read" if "tsg_member" on "group";
    "read_data" if "tsg_member" on "group";
    "write_data" if "tsg_member" on "group";
}

has_relation(group: TimeseriesGroup, "group", ts: Timeseries) if
    group = ts.group;


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


resource EventChannel {
    permissions = [
        "create", "read", "update", "delete",
        "create_events", "read_events", "update_events", "delete_events",
    ];

    roles = ["ec_member"];

    "read" if "ec_member";
    "read_events" if "ec_member";
    "create_events" if "ec_member";
    "update_events" if "ec_member";
    "delete_events" if "ec_member";
}

has_role(user: UserActor, "ec_member", ec: EventChannel) if
    ecbu in ec.event_channels_by_users and
    user = ecbu.user;


resource EventChannelByCampaign {
    permissions = ["create", "read", "update", "delete"];
    relations = {
        campaign: Campaign
    };

    "read" if "c_member" on "campaign";
}

has_relation(campaign: Campaign, "campaign", ecbc: EventChannelByCampaign) if
  campaign = ecbc.campaign;


resource EventChannelByUser {
    permissions = ["create", "read", "update", "delete"];
    roles = ["ecbu_owner"];

    "read" if "ecbu_owner";
}

has_role(user: UserActor, "ecbu_owner", ecbu: EventChannelByUser) if
    user = ecbu.user;


resource Event {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: UserActor, "create", event:Event) if
    has_permission(user, "create_events", event.channel);
has_permission(user: UserActor, "read", event:Event) if
    has_permission(user, "read_events", event.channel);
has_permission(user: UserActor, "update", event:Event) if
    has_permission(user, "update_events", event.channel);
has_permission(user: UserActor, "delete", event:Event) if
    has_permission(user, "delete_events", event.channel);
