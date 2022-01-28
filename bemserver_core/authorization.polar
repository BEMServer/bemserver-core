# TODO: Investigate potential Oso issue
# We should't have to write "c_member" and "tg_member", Oso should infer which
# has_role rule matches which resource type.
# "self" could be impacted as well

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
    has_role(user, "self", ubc);


resource UserByCampaign {
    permissions = ["create", "read", "update", "delete"];
    roles = ["self"];

    "read" if "self";
}

has_role(user: UserActor, "self", ubc: UserByCampaign) if
    user = ubc.user;


resource TimeseriesGroupByCampaign {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: UserActor, "read", tgbc: TimeseriesGroupByCampaign) if
    has_role(user, "c_member", tgbc.campaign) and
    has_role(user, "tg_member", tgbc.timeseries_group);


resource TimeseriesGroup {
    permissions = ["create", "read", "update", "delete"];
    roles = ["tg_member"];

    "read" if "tg_member";
}

has_role(user: UserActor, "tg_member", tg: TimeseriesGroup) if
    tgbu in tg.timeseries_groups_by_users and
    has_role(user, "self", tgbu);


resource TimeseriesGroupByUser {
    permissions = ["create", "read", "update", "delete"];
    roles = ["self"];

    "read" if "self";
}

has_role(user: UserActor, "self", tgbu: TimeseriesGroupByUser) if
    user = tgbu.user;


resource Timeseries {
    permissions = ["create", "read", "update", "delete", "read_data", "write_data"];
    relations = {
        group: TimeseriesGroup
    };

    "read" if "tg_member" on "group";
    "read_data" if "tg_member" on "group";
    "write_data" if "tg_member" on "group";
}

has_relation(group: TimeseriesGroup, "group", ts: Timeseries) if
    group = ts.group;


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
    has_role(user, "self", ecbu);


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
    roles = ["self"];

    "read" if "self";
}

has_role(user: UserActor, "self", ecbu: EventChannelByUser) if
    user = ecbu.user;


resource TimeseriesEvent {
    permissions = ["create", "read", "update", "delete"];
}

has_permission(user: UserActor, "create", event:TimeseriesEvent) if
    has_permission(user, "create_events", event.channel);
has_permission(user: UserActor, "read", event:TimeseriesEvent) if
    has_permission(user, "read_events", event.channel);
has_permission(user: UserActor, "update", event:TimeseriesEvent) if
    has_permission(user, "update_events", event.channel);
has_permission(user: UserActor, "delete", event:TimeseriesEvent) if
    has_permission(user, "delete_events", event.channel);
